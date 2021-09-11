// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.Workers
{
    internal class WorkerConcurrencyManager : IHostedService, IDisposable
    {
        private readonly TimeSpan _logStateInterval = TimeSpan.FromSeconds(60);
        private readonly IOptions<WorkerConcurrencyOptions> _workerConcurrencyOptions;
        private readonly ILogger _logger;
        private readonly IFunctionInvocationDispatcherFactory _functionInvocationDispatcherFactory;

        private IFunctionInvocationDispatcher _functionInvocationDispatcher;
        private System.Timers.Timer _timer;
        private Stopwatch _addWorkerStopwatch = Stopwatch.StartNew();
        private Stopwatch _logStateStopWatch = Stopwatch.StartNew();
        private bool _disposed = false;

        public WorkerConcurrencyManager(IFunctionInvocationDispatcherFactory functionInvocationDispatcherFactory,
            IOptions<WorkerConcurrencyOptions> workerConcurrencyOptions, ILoggerFactory loggerFactory)
        {
            _workerConcurrencyOptions = workerConcurrencyOptions ?? throw new ArgumentNullException(nameof(workerConcurrencyOptions));
            _functionInvocationDispatcherFactory = functionInvocationDispatcherFactory ?? throw new ArgumentNullException(nameof(functionInvocationDispatcherFactory));

            _logger = loggerFactory?.CreateLogger(LogCategories.Concurrency);
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_workerConcurrencyOptions.Value.DynamicConcurrencyEnabled)
            {
                _functionInvocationDispatcher = _functionInvocationDispatcherFactory.GetFunctionDispatcher();

                if (_functionInvocationDispatcher is HttpFunctionInvocationDispatcher)
                {
                    _logger.LogDebug($"Http dynamic worker concurrency is not supported.");
                    return;
                }

                _logger.LogDebug($"Starting dynamic worker concurrency monitoring.");
                _timer = new System.Timers.Timer()
                {
                    AutoReset = false,
                    Interval = _workerConcurrencyOptions.Value.CheckInterval.TotalMilliseconds,
                };

                _timer.Elapsed += OnTimer;
                _timer.Start();
            }
            else
            {
                _logger.LogDebug($"Dynamic worker concurrency is disabled.");
            }

            await Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (_workerConcurrencyOptions.Value.DynamicConcurrencyEnabled && _timer != null)
            {
                _logger.LogDebug("Stopping dynamic worker concurrency monitoring.");
                _timer.Stop();
            }
            return Task.CompletedTask;
        }

        internal async void OnTimer(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                var workerStatuses = await _functionInvocationDispatcher.GetWorkerStatusesAsync();

                if (NewWorkerIsRequired(workerStatuses, _addWorkerStopwatch.Elapsed))
                {
                    await _functionInvocationDispatcher.StartWorkerChannel();
                    _logger.LogDebug("New worker is added.");
                    _addWorkerStopwatch.Restart();
                }
            }
            catch (Exception ex)
            {
                // don't allow background exceptions to escape
                _logger.LogError(ex.ToString());
            }
            _timer.Start();
        }

        internal bool NewWorkerIsRequired(IDictionary<string, WorkerStatus> workerStatuses, TimeSpan timeSinceLastNewWorker)
        {
            if (timeSinceLastNewWorker < _workerConcurrencyOptions.Value.AdjustmentPeriod)
            {
                return false;
            }

            bool result = false;
            if (workerStatuses.All(x => x.Value.IsReady))
            {
                // Check how many channels are overloaded
                List<WorkerStatusDetails> descriptions = new List<WorkerStatusDetails>();
                foreach (string key in workerStatuses.Keys)
                {
                    WorkerStatus workerStatus = workerStatuses[key];
                    bool overloaded = IsOverloaded(workerStatus);
                    descriptions.Add(new WorkerStatusDetails()
                    {
                        WorkerId = key,
                        WorkerStatus = workerStatus,
                        IsOverloaded = overloaded
                    });
                }

                int overloadedCount = descriptions.Where(x => x.IsOverloaded == true).Count();
                if (overloadedCount > 0)
                {
                    if (workerStatuses.Count() < _workerConcurrencyOptions.Value.MaxWorkerCount)
                    {
                        _logger.LogDebug($"Adding a new worker, overloaded workers = {overloadedCount}, initialized workers = {workerStatuses.Count()} ");
                        result = true;
                    }
                }

                if (result == true || _logStateStopWatch.Elapsed > _logStateInterval)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (WorkerStatusDetails description in descriptions)
                    {
                        sb.Append(FormatWorkerDescription(description));
                        sb.Append(Environment.NewLine);
                    }
                    _logStateStopWatch.Restart();
                    _logger.LogDebug(sb.ToString());
                }
            }

            return result;
        }

        internal bool IsOverloaded(WorkerStatus status)
        {
            if (status.LatencyHistory.Count() >= _workerConcurrencyOptions.Value.HistorySize)
            {
                int overloadedCount = status.LatencyHistory.Where(x => x.TotalMilliseconds >= _workerConcurrencyOptions.Value.LatencyThreshold.TotalMilliseconds).Count();
                double proportion = (double)overloadedCount / _workerConcurrencyOptions.Value.HistorySize;

                return proportion >= _workerConcurrencyOptions.Value.NewWorkerThreshold;
            }
            return false;
        }

        internal string FormatWorkerDescription(WorkerStatusDetails desc)
        {
            string formattedLoadHistory = string.Empty, formattedLatencyHistory = string.Empty;
            double latencyAvg = 0, latencyMax = 0;
            if (desc.WorkerStatus != null && desc.WorkerStatus.LatencyHistory != null)
            {
                formattedLatencyHistory = string.Join(",", desc.WorkerStatus.LatencyHistory);
                latencyMax = desc.WorkerStatus.LatencyHistory.Select(x => x.TotalMilliseconds).Max();
                if (desc.WorkerStatus.LatencyHistory.Count() > 1)
                {
                    latencyAvg = desc.WorkerStatus.LatencyHistory.Select(x => x.TotalMilliseconds).Average();
                }
            }

            return $@"Worker process stats: ProcessId={desc.WorkerId}, Overloaded={desc.IsOverloaded} 
LatencyHistory=({formattedLatencyHistory}), AvgLatency={latencyAvg}, MaxLatency={latencyMax}";
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _timer?.Dispose();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        internal class WorkerStatusDetails
        {
            public string WorkerId { get; set; }

            public WorkerStatus WorkerStatus { get; set; }

            public bool IsOverloaded { get; set; }
        }
    }
}
