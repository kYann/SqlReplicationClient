using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers.DriversImpl
{
    public class NpgsqlServerUpChecker : ServerUpChecker
    {
        public NpgsqlServerUpChecker(Func<IDbConnection> cnxFactory)
            : base(cnxFactory)
        {
        }

        protected override bool isServerUp(System.Data.IDbConnection cnx)
        {
            if (base.isServerUp(cnx))
            {
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = "SELECT now()"; // execute dumb command
                    cmd.ExecuteScalar();
                }
            }
            return true;
        }
    }
}
