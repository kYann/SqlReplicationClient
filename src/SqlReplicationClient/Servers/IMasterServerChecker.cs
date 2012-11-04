using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers
{
    public interface IMasterServerChecker
    {
        bool IsMaster(string connectionString);
    }
}
