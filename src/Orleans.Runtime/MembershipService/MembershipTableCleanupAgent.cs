using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Internal;

namespace Orleans.Runtime.MembershipService
{
    /// <summary>
    /// Responsible for cleaning up dead membership table entries.
    /// </summary>
    internal class MembershipTableCleanupAgent : IHealthCheckParticipant, ILifecycleParticipant<ISiloLifecycle>, ILifecycleObserver
    {
        private readonly ClusterMembershipOptions clusterMembershipOptions;
        private readonly IMembershipTable membershipTableProvider;
        private readonly ILogger<MembershipTableCleanupAgent> log;
        private readonly IAsyncTimer cleanupDefunctSilosTimer;
        private Task _runTask;

        public MembershipTableCleanupAgent(
            IOptions<ClusterMembershipOptions> clusterMembershipOptions,
            IMembershipTable membershipTableProvider,
            ILogger<MembershipTableCleanupAgent> log,
            IAsyncTimerFactory timerFactory)
        {
            this.clusterMembershipOptions = clusterMembershipOptions.Value;
            this.membershipTableProvider = membershipTableProvider;
            this.log = log;
            if (this.clusterMembershipOptions.DefunctSiloCleanupPeriod.HasValue)
            {
                this.cleanupDefunctSilosTimer = timerFactory.Create(
                    this.clusterMembershipOptions.DefunctSiloCleanupPeriod.Value,
                    nameof(CleanupDefunctSilos));
            }
        }

        private async Task CleanupDefunctSilos()
        {
            if (!this.clusterMembershipOptions.DefunctSiloCleanupPeriod.HasValue)
            {
                if (this.log.IsEnabled(LogLevel.Debug))
                {
                    this.log.LogDebug($"Membership table cleanup is disabled due to {nameof(ClusterMembershipOptions)}.{nameof(ClusterMembershipOptions.DefunctSiloCleanupPeriod)} not being specified");
                }

                return;
            }

            if (this.log.IsEnabled(LogLevel.Debug)) this.log.LogDebug("Starting membership table cleanup agent");
            try
            {
                var period = this.clusterMembershipOptions.DefunctSiloCleanupPeriod.Value;

                // The first cleanup should be scheduled for shortly after silo startup.
                var delay = ThreadSafeRandom.NextTimeSpan(TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(10));
                while (await this.cleanupDefunctSilosTimer.NextTick(delay))
                {
                    // Select a random time within the next window.
                    // The purpose of this is to add jitter to a process which could be affected by contention with other silos.
                    delay = ThreadSafeRandom.NextTimeSpan(period, period + TimeSpan.FromMinutes(5));
                    try
                    {
                        var dateLimit = DateTime.UtcNow - this.clusterMembershipOptions.DefunctSiloExpiration;
                        await this.membershipTableProvider.CleanupDefunctSiloEntries(dateLimit);
                    }
                    catch (Exception exception) when (exception is NotImplementedException || exception is MissingMethodException)
                    {
                        this.cleanupDefunctSilosTimer.Dispose();
                        this.log.LogWarning(
                            (int)ErrorCode.MembershipCleanDeadEntriesFailure,
                            $"{nameof(IMembershipTable.CleanupDefunctSiloEntries)} operation is not supported by the current implementation of {nameof(IMembershipTable)}. Disabling the timer now.");
                        return;
                    }
                    catch (Exception exception)
                    {
                        this.log.LogError((int)ErrorCode.MembershipCleanDeadEntriesFailure, "Failed to clean up defunct membership table entries: {Exception}", exception);
                    }
                }
            }
            finally
            {
                if (this.log.IsEnabled(LogLevel.Debug)) this.log.LogDebug("Stopped membership table cleanup agent");
            }
        }

        void ILifecycleParticipant<ISiloLifecycle>.Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(MembershipTableCleanupAgent), ServiceLifecycleStage.Active, this);
        }

        Task ILifecycleObserver.OnStart(CancellationToken ct)
        {
            _runTask = Task.Run(CleanupDefunctSilos);
            return Task.CompletedTask;
        }

        Task ILifecycleObserver.OnStop(CancellationToken ct)
        {
            this.cleanupDefunctSilosTimer?.Dispose();
            _runTask?.Ignore();
            return _runTask.WhenCompletedOrCanceled(ct).AsTask();
        }

        bool IHealthCheckable.CheckHealth(DateTime lastCheckTime, out string reason)
        {
            if (cleanupDefunctSilosTimer is IAsyncTimer timer)
            {
                return timer.CheckHealth(lastCheckTime, out reason);
            }

            reason = default;
            return true;
        }
    }
}
