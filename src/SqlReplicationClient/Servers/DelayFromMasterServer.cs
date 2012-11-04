using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers
{
    public abstract class DelayFromMasterServerChecker : IDelayFromMasterServerChecker
    {
        protected readonly Func<IDbConnection> cnxFactory;

        public DelayFromMasterServerChecker(Func<IDbConnection> cnxFactory)
        {
            this.cnxFactory = cnxFactory;
        }

        public abstract void SetupMaster(string masterConnectionString);

        public abstract TimeSpan GetDelayFromMaster(string slaveConnectionString);
    }
}
