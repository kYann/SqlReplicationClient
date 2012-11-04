using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security;
using System.Text;

namespace SqlReplicationClient.Servers.DriversImpl
{
    public class NpgsqlDelayFromMasterServerChecker : DelayFromMasterServerChecker
    {
        string lastMasterConnectionString;

        public NpgsqlDelayFromMasterServerChecker(Func<IDbConnection> cnxFactory)
            : base(cnxFactory)
        {
        }

        protected void InsertInMaster(string masterConnectionString)
        {
            using (var cnx = this.cnxFactory())
            {
                cnx.ConnectionString = masterConnectionString;
                cnx.Open();
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = "DELETE FROM client_monitoring.delay; INSERT INTO client_monitoring.delay VALUES (now())";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        protected void CheckIfTableExistAndHasRight(string masterConnectionString)
        {
            using (var cnx = this.cnxFactory())
            {
                cnx.ConnectionString = masterConnectionString;
                cnx.Open();
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = @"SELECT EXISTS(SELECT 1 FROM information_schema.tables 
                        WHERE table_schema='client_monitoring' AND 
                        table_name='delay');";
                    var result = cmd.ExecuteScalar();
                    if (result is bool && !((bool)result))
                    {
                        throw new NotImplementedException("Table client_monitoring.delay does not exists in database. "
                            + "You must launch setup.sql to create monitoring table");
                    }
                }
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = @"SELECT has_table_privilege('client_monitoring.delay', 'INSERT, DELETE')";
                    var result = cmd.ExecuteScalar();
                    if (result is bool && !((bool)result))
                    {
                        throw new SecurityException("Current user doesn't have enough right on client_monitoring.delay. "
                            + "You must set SELECT, INSERT, DELETE on client_monitoring.delay for current user");
                    }
                }
            }
        }

        /// <summary>
        /// Insert now() in master so we can check time difference on slave
        /// </summary>
        /// <param name="masterConnectionString">master database connection string</param>
        /// <exception cref="System.NotImplementedException">
        /// Throw not implemented exception if sql table are not available
        /// </exception>
        /// <exception cref="System.Security.SecurityException">
        /// Throw security exception if given database user can't write to monitoring table
        /// </exception>
        public override void SetupMaster(string masterConnectionString)
        {
            // if master has changed, we must check table exist and right
            if (lastMasterConnectionString != masterConnectionString)
                this.CheckIfTableExistAndHasRight(masterConnectionString);
            lastMasterConnectionString = masterConnectionString;
            this.InsertInMaster(masterConnectionString);
        }

        /// <summary>
        /// Get difference in time between master and slave
        /// </summary>
        /// <param name="slaveConnectionString">slave database connection string</param>
        /// <returns>Timespan representing replication time difference between the two server</returns>
        public override TimeSpan GetDelayFromMaster(string slaveConnectionString)
        {
            using (var cnx = this.cnxFactory())
            {
                cnx.Open();
                using (var cmd = cnx.CreateCommand())
                {
                    cmd.CommandText = @"select now()-max(master_datetime) from client_monitoring.delay";
                    var result = cmd.ExecuteScalar();
                    if (result == null || result == DBNull.Value)
                        return TimeSpan.MaxValue; // if no value then maybe slave is not in sync at all
                    return ((TimeSpan)result);
                }
            }
        }
    }
}
