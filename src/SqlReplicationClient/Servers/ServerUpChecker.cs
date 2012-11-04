using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers
{
    public class ServerUpChecker : IServerUpChecker
    {
        protected readonly Func<IDbConnection> cnxFactory;

        public ServerUpChecker(Func<IDbConnection> cnxFactory)
        {
            this.cnxFactory = cnxFactory;
        }

        public virtual bool IsServerUp(string connectionString)
        {
            try
            {
                using (var cnx = this.cnxFactory())
                {
                    cnx.ConnectionString = connectionString;
                    cnx.Open();
                    return this.isServerUp(cnx);
                }
            }
            catch (Exception exc)
            {
                return false;
            }
        }

        protected virtual bool isServerUp(IDbConnection cnx)
        {
            // wait 200ms for connection to be in open state
            for (int i = 0; i < 20; i++)
            {
                if (cnx.State == ConnectionState.Open)
                    return true;
                Thread.Sleep(10);
            }
            return false;
        }
    }
}
