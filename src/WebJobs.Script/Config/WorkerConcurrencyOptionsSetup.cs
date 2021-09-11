// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.Config
{
    internal class WorkerConcurrencyOptionsSetup : IConfigureOptions<WorkerConcurrencyOptions>
    {
        private readonly IConfiguration _configuration;
        private readonly IEnvironment _environment;

        public WorkerConcurrencyOptionsSetup(IConfiguration configuration, IEnvironment environment)
        {
            _configuration = configuration;
            _environment = environment;
        }

        public void Configure(WorkerConcurrencyOptions options)
        {
            if (!string.IsNullOrEmpty(_environment.GetEnvironmentVariable(RpcWorkerConstants.FunctionsWorkerProcessCountSettingName))
                || !string.IsNullOrEmpty(_environment.GetEnvironmentVariable(RpcWorkerConstants.PythonThreadpoolThreadCount))
                || !string.IsNullOrEmpty(_environment.GetEnvironmentVariable(RpcWorkerConstants.PSWorkerInProcConcurrencyUpperBound)))
            {
                options.DynamicConcurrencyEnabled = false;
            }
            else
            {
                // Configure dynamic worker concurrency options from IConfiguration
                _configuration.GetSection(nameof(WorkerConcurrencyOptions)).Bind(options);

                if (options.MaxWorkerCount == 0)
                {
                    options.MaxWorkerCount = (_environment.GetEffectiveCoresCount() * 2) + 2;
                }
            }
        }
    }
}
