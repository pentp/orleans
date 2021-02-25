using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Internal;
using Orleans.Runtime.MembershipService;
using Orleans.Runtime.Utilities;

namespace Orleans.Runtime
{
    internal class ClusterMembershipService : IClusterMembershipService, ILifecycleParticipant<ISiloLifecycle>, ILifecycleObserver, IDisposable
    {
        private readonly AsyncEnumerable<ClusterMembershipSnapshot> updates;
        private readonly MembershipTableManager membershipTableManager;
        private readonly ILogger<ClusterMembershipService> log;
        private readonly IFatalErrorHandler fatalErrorHandler;
        private readonly CancellationTokenSource cancellation = new();
        private Task updateTask = Task.CompletedTask;
        private ClusterMembershipSnapshot snapshot;

        public ClusterMembershipService(
            MembershipTableManager membershipTableManager,
            ILogger<ClusterMembershipService> log,
            IFatalErrorHandler fatalErrorHandler)
        {
            this.snapshot = membershipTableManager.MembershipTableSnapshot.CreateClusterMembershipSnapshot();
            this.updates = new AsyncEnumerable<ClusterMembershipSnapshot>(
                (previous, proposed) => proposed.Version == MembershipVersion.MinValue || proposed.Version > previous.Version,
                this.snapshot)
            {
                OnPublished = update => Interlocked.Exchange(ref this.snapshot, update)
            };
            this.membershipTableManager = membershipTableManager;
            this.log = log;
            this.fatalErrorHandler = fatalErrorHandler;
        }

        public ClusterMembershipSnapshot CurrentSnapshot
        {
            get
            {
                var tableSnapshot = this.membershipTableManager.MembershipTableSnapshot;
                if (this.snapshot.Version == tableSnapshot.Version)
                {
                    return this.snapshot;
                }

                this.updates.TryPublish(tableSnapshot.CreateClusterMembershipSnapshot());
                return this.snapshot;
            }
        }

        public IAsyncEnumerable<ClusterMembershipSnapshot> MembershipUpdates => this.updates;

        public ValueTask Refresh(MembershipVersion targetVersion)
        {
            if (targetVersion != default && targetVersion != MembershipVersion.MinValue && this.snapshot.Version >= targetVersion)
                return default;

            return RefreshAsync(targetVersion);

            async ValueTask RefreshAsync(MembershipVersion v)
            {
                var didRefresh = false;
                do
                {
                    if (!didRefresh || this.membershipTableManager.MembershipTableSnapshot.Version < v)
                    {
                        await this.membershipTableManager.Refresh();
                        didRefresh = true;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(10));
                } while (this.snapshot.Version < v || this.snapshot.Version < this.membershipTableManager.MembershipTableSnapshot.Version);
            }
        }

        public Task<bool> TryKill(SiloAddress siloAddress) => this.membershipTableManager.TryKill(siloAddress);

        private async Task ProcessMembershipUpdates()
        {
            try
            {
                if (this.log.IsEnabled(LogLevel.Debug)) this.log.LogDebug("Starting to process membership updates");
                await foreach (var tableSnapshot in this.membershipTableManager.MembershipTableUpdates.WithCancellation(cancellation.Token))
                {
                    this.updates.TryPublish(tableSnapshot.CreateClusterMembershipSnapshot());
                }
            }
            catch (Exception exception) when (this.fatalErrorHandler.IsUnexpected(exception))
            {
                this.log.LogError("Error processing membership updates: {Exception}", exception);
                this.fatalErrorHandler.OnFatalException(this, nameof(ProcessMembershipUpdates), exception);
            }
            finally
            {
                if (this.log.IsEnabled(LogLevel.Debug)) this.log.LogDebug("Stopping membership update processor");
            }
        }

        void ILifecycleParticipant<ISiloLifecycle>.Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(ClusterMembershipService), ServiceLifecycleStage.RuntimeInitialize, this);
        }

        Task ILifecycleObserver.OnStart(CancellationToken ct)
        {
            updateTask = Task.Run(ProcessMembershipUpdates);
            return Task.CompletedTask;
        }

        async Task ILifecycleObserver.OnStop(CancellationToken ct)
        {
            cancellation?.Cancel();
            await updateTask.WhenCompletedOrCanceled(ct, gracePeriod: ClusterMembershipOptions.ClusteringShutdownGracePeriod);
        }

        void IDisposable.Dispose()
        {
            this.updates.Dispose();
        }
    }
}
