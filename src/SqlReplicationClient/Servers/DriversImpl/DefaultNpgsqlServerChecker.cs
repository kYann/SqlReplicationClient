using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace SqlReplicationClient.Servers.DriversImpl
{
    public class DefaultNpgsqlServerChecker :
        ServerChecker
    {
        public DefaultNpgsqlServerChecker(Func<IDbConnection> cnxFactory)
            : base(
                new NpgsqlServerUpChecker(cnxFactory),
                new NpgsqlMasterServerChecker(cnxFactory),
                new NpgsqlDelayFromMasterServerChecker(cnxFactory))
        {
        }
    }
}
