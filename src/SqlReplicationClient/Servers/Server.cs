using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers
{
    public class Server
    {
        public Server(string connectionString)
        {
            this.ConnectionString = connectionString;
            this.DelayFromMaster = TimeSpan.Zero;
        }

        public string ConnectionString { get; protected set; }

        public bool IsUp { get; set; }

        public bool IsWritable { get; set; }

        public DateTime LastAlive { get; set; }

        public TimeSpan DelayFromMaster { get; set; }
    }
}
