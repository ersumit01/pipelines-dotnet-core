using System.Collections.Concurrent;
using System.Threading.Tasks;
using Aerospike.Client;

public class PartitionedWorkItemScanner
{
    private readonly IAerospikeClient _client;
    private readonly string _namespace;
    private readonly string _set;
    private int _workItemId = 0;

    public PartitionedWorkItemScanner(IAerospikeClient client, string ns, string set)
    {
        _client = client;
        _namespace = ns;
        _set = set;
    }

    public IEnumerable<WorkItem> ScanWorkItems(int batchSize = 1000, int parallelism = 4)
    {
        var outputQueue = new BlockingCollection<WorkItem>(boundedCapacity: 100);
        var errorQueue = new ConcurrentQueue<string>();
        var partitionRanges = PartitionFilter.All().GetRanges(parallelism);

        Parallel.ForEach(partitionRanges, range =>
        {
            var policy = new ScanPolicy { includeBinData = false };
            var pf = new PartitionFilter(range.begin, range.count);
            var buffer = new List<string>(batchSize);

            try
            {
                _client.ScanPartitions(policy, pf, _namespace, _set, (key, record) =>
                {
                    try
                    {
                        if (key.userKey == null)
                        {
                            errorQueue.Enqueue($"Null userKey at digest: {BitConverter.ToString(key.digest)}");
                            return;
                        }

                        var pid = key.userKey.ToString();

                        lock (buffer)
                        {
                            buffer.Add(pid);

                            if (buffer.Count >= batchSize)
                            {
                                var workItem = new WorkItem
                                {
                                    WorkItemId = Interlocked.Increment(ref _workItemId),
                                    Pids = new List<string>(buffer)
                                };
                                buffer.Clear();
                                outputQueue.Add(workItem);
                            }
                        }
                    }
                    catch (Exception recordEx)
                    {
                        errorQueue.Enqueue($"Error processing record {key?.userKey ?? "[null key]"}: {recordEx.Message}");
                    }
                });

                // Emit leftover records
                lock (buffer)
                {
                    if (buffer.Count > 0)
                    {
                        var workItem = new WorkItem
                        {
                            WorkItemId = Interlocked.Increment(ref _workItemId),
                            Pids = new List<string>(buffer)
                        };
                        outputQueue.Add(workItem);
                    }
                }
            }
            catch (Exception scanEx)
            {
                errorQueue.Enqueue($"Scan failed for partition range [{range.begin}-{range.begin + range.count - 1}]: {scanEx.Message}");
            }
        });

        outputQueue.CompleteAdding();

        foreach (var workItem in outputQueue.GetConsumingEnumerable())
        {
            yield return workItem;
        }

        // After scan is done, report errors (optional)
        if (!errorQueue.IsEmpty)
        {
            Console.WriteLine($"\n⚠ Scan completed with {errorQueue.Count} errors:");
            foreach (var err in errorQueue)
            {
                Console.WriteLine($"- {err}");
            }
        }
    }
}