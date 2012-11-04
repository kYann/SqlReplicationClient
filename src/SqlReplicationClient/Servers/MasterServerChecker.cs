using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Servers
{
    public abstract class MasterServerChecker : IMasterServerChecker
    {
        protected readonly Func<IDbConnection> cnxFactory;

        public MasterServerChecker(Func<IDbConnection> cnxFactory)
        {
            this.cnxFactory = cnxFactory;
        }

        public virtual bool IsMaster(string connectionString)
        {
            try
            {
                using (var cnx = this.cnxFactory())
                {
                    cnx.ConnectionString = connectionString;
                    cnx.Open();
                    return this.isMaster(cnx);
                }
            }
            catch (Exception exc)
            {
                return false;
            }
        }

        protected abstract bool isMaster(IDbConnection cnx);
    }
}
