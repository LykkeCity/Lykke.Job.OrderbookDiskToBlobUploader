using Autofac;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;
using Lykke.Job.OrderbookDiskToBlobUploader.Settings;
using Lykke.Job.OrderbookDiskToBlobUploader.Services;
using Lykke.Job.OrderbookDiskToBlobUploader.PeriodicalHandlers;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Modules
{
    public class JobModule : Module
    {
        private readonly OrderbookDiskToBlobUploaderSettings _settings;
        private readonly ILog _log;

        public JobModule(OrderbookDiskToBlobUploaderSettings settings, ILog log)
        {
            _settings = settings;
            _log = log;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();

            builder.RegisterType<BlobSaver>()
                .As<IBlobSaver>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.BlobConnectionString));

            builder.RegisterType<DirectoryProcessor>()
                .As<IDirectoryProcessor>()
                .SingleInstance();

            builder.RegisterType<MainPeriodicalHandler>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.DiskPath))
                .WithParameter(TypedParameter.From(_settings.MaxFilesInBatch));
        }
    }
}
