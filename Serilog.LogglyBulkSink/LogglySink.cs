using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.LogglyBulkSink
{
    public class LogglySink : PeriodicBatchingSink
    {
        private readonly string _logglyUrl;
        public const string LogglyUriFormat = "https://logs-01.loggly.com/bulk/{0}/tag/{1}";
        public const double MaxBulkBytes = 4.5 * 1024 * 1024;
        private readonly bool _includeDiagnostics;
        private HttpClient _httpClient = new HttpClient();
        private ConcurrentQueue<Task> _batchQueue = new ConcurrentQueue<Task>();
        private int _emitCount = 0;
        private readonly int _maxConcurrentBatches;

        public LogglySink(string logglyKey, string[] tags, int batchSizeLimit, TimeSpan period, bool includeDiagnostics = false, int maxConcurrentBatches = 1)
            : base(batchSizeLimit, period)
        {
            _includeDiagnostics = includeDiagnostics;
            _logglyUrl = string.Format(LogglyUriFormat, logglyKey, string.Join(",", tags));
            _maxConcurrentBatches = maxConcurrentBatches;
        }

        public LogglySink(string logglyKey, string[] tags, int batchSizeLimit, TimeSpan period, int queueLimit, bool includeDiagnostics = false, int maxConcurrentBatches = 1)
            : base(batchSizeLimit, period, queueLimit)
        {
            _includeDiagnostics = includeDiagnostics;
            _logglyUrl = string.Format(LogglyUriFormat, logglyKey, string.Join(",", tags));
            _maxConcurrentBatches = maxConcurrentBatches;
        }

        protected override Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            _batchQueue.Enqueue(ProcessBatchAsync(events));

            Task result;

            if (Interlocked.Increment(ref _emitCount) >= _maxConcurrentBatches && _batchQueue.TryDequeue(out result))
            {
                return result;
            }
            else
            {
                return Task.CompletedTask;
            }
        }

        private async Task ProcessBatchAsync(IEnumerable<LogEvent> events)
        {
            foreach (var content in ChunkEvents(events))
            {
                if (content == null)
                {
                    continue;
                }

                try
                {
                    var response = await _httpClient.PostAsync(_logglyUrl, content).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                }
                catch (Exception ex)
                {
                    Trace.WriteLine($"Exception posting to loggly {ex}");
                    Debugging.SelfLog.WriteLine($"Exception posting to loggly {ex}");
                }
            }
        }

        public IEnumerable<StringContent> ChunkEvents(IEnumerable<LogEvent> events)
        {
            if (events == null)
            {
                yield break;
            }

            var jsons = events.Select(EventToJson).Where(_ => !string.IsNullOrWhiteSpace(_)).ToList();

            var bytes = 0;
            int page = 0;
            var chunk = new List<string>();

            foreach (var json in jsons)
            {
                if (bytes > MaxBulkBytes)
                {
                    yield return PackageContent(chunk, bytes, page);
                    bytes = 0;
                    page++;
                    chunk.Clear();
                }

                bytes += Encoding.UTF8.GetByteCount(json) + 1;
                chunk.Add(json);
            }

            yield return PackageContent(chunk, bytes, page, _includeDiagnostics);
        }

        public static StringContent PackageContent(List<string> jsons, int bytes, int page, bool includeDiagnostics = false)
        {
            if (includeDiagnostics)
            {
                var diagnostic = JsonConvert.SerializeObject(new
                {
                    Event = "LogglyDiagnostics",
                    Trace = $"EventCount={jsons.Count}, ByteCount={bytes}, PageCount={page}"
                });
                jsons.Add(diagnostic);
            }

            return new StringContent(string.Join("\n", jsons), Encoding.UTF8, "application/json");
        }

        public static string EventToJson(LogEvent logEvent)
        {
            if (logEvent == null)
            {
                throw new ArgumentNullException(nameof(logEvent));
            }

            var payload = new Dictionary<string, object>();
            try
            {
                foreach (var key in logEvent.Properties.Keys)
                {
                    int dummy;
                    if (Int32.TryParse(key, out dummy)) continue;
                    var propertyValue = logEvent.Properties[key];
                    var simpleValue = SerilogPropertyFormatter.Simplify(propertyValue);
                    var safeKey = key.Replace(" ", "").Replace(":", "").Replace("-", "").Replace("_", "");
                    AddIfNotContains(payload, safeKey, simpleValue);
                }

                AddIfNotContains(payload, "Level", logEvent.Level.ToString());
                AddIfNotContains(payload, "Timestamp", logEvent.Timestamp);
                AddIfNotContains(payload, "Raw", logEvent.RenderMessage());

                if (logEvent.Exception != null)
                {
                    AddIfNotContains(payload, "Exception", logEvent.Exception);
                }

                var result = JsonConvert.SerializeObject(payload,
                    new JsonSerializerSettings
                    {
                        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                    });
                return result;
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error extracting json from logEvent {ex}");
            }
            return null;
        }

        public static void AddIfNotContains<TKey, TValue>(IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
        {
            if (dictionary.ContainsKey(key)) return;
            dictionary[key] = value;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                var prevContext = SynchronizationContext.Current;
                SynchronizationContext.SetSynchronizationContext(null);
                try
                {
                    Task result;
                    while (_batchQueue.TryDequeue(out result))
                    {
                        result.Wait();
                    }
                }
                finally
                {
                    SynchronizationContext.SetSynchronizationContext(prevContext);
                }

                _httpClient.Dispose();
            }
        }
    }
}
