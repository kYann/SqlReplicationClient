using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers.DriversImpl
{
    public class NpgsqlMasterServerChecker : MasterServerChecker
    {
        public NpgsqlMasterServerChecker(Func<IDbConnection> cnxFactory)
            : base(cnxFactory)
        {
        }

        protected override bool isMaster(System.Data.IDbConnection cnx)
        {
            using (var cmd = cnx.CreateCommand())
            {
                cmd.CommandText = "SELECT pg_is_in_recovery()";
                var result = cmd.ExecuteScalar();

                if (result == null)
                    return true;
                if ((bool)result) // if it's in recovery then it's standby
                    return false;
                return true;
            }
        }
    }
}
