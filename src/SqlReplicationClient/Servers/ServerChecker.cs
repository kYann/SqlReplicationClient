using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers
{
    public class ServerChecker
    {
        IServerUpChecker serverUpChecker;
        IMasterServerChecker masterServerChecker;
        IDelayFromMasterServerChecker delayFromMasterServerChecker;

        public ServerChecker(IServerUpChecker serverUpChecker,
            IMasterServerChecker masterServerChecker,
            IDelayFromMasterServerChecker delayFromMasterServerChecker)
        {
            this.serverUpChecker = serverUpChecker;
            this.masterServerChecker = masterServerChecker;
            this.delayFromMasterServerChecker = delayFromMasterServerChecker;
        }

        public virtual void DoCheckupOnAllServers(IEnumerable<Server> servers)
        {
            Parallel.ForEach(servers, server =>
            {
                server.IsUp = this.serverUpChecker.IsServerUp(server.ConnectionString);
                server.IsWritable = this.masterServerChecker.IsMaster(server.ConnectionString);
                if (server.IsUp)
                    server.LastAlive = DateTime.Now;
            });

            var master = servers.FirstOrDefault(c => c.IsWritable);

            if (master != null)
            {
                // we won't check delay for master (obvious) so we set it to zero
                master.DelayFromMaster = TimeSpan.Zero;
                this.delayFromMasterServerChecker.SetupMaster(master.ConnectionString);
                Parallel.ForEach(servers.Where(c => !c.IsWritable && c.IsUp), server =>
                {
                    server.DelayFromMaster =
                        this.delayFromMasterServerChecker.GetDelayFromMaster(server.ConnectionString);
                });
            }
        }
    }
}
