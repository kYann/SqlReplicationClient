using SqlReplicationClient.Sessions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Ado
{
    public class ReplicationConnection : IDbConnection
    {
        readonly IConnectionManager connectionManager;
        readonly ICommandTypeAnalyser cmdTypeAnalyzer;

        IDbConnection writableConnection;
        List<IDbConnection> connections = new List<IDbConnection>();

        bool shouldOpen = false;
        string databaseName = null;

        ReplicationTransaction transaction;
        IsolationLevel? il;

        public ReplicationConnection(IConnectionManager connectionManager) :
            this(connectionManager, new DefaultCommandTypeAnalyser())
        {
        }

        public ReplicationConnection(IConnectionManager connectionManager,
            ICommandTypeAnalyser cmdTypeAnalyzer)
        {
            this.connectionManager = connectionManager;
            this.cmdTypeAnalyzer = cmdTypeAnalyzer;
        }

        protected virtual void BeginTransaction(IDbConnection cnx, bool openByWrite)
        {
            if (this.transaction != null)
            {
                // we do one transaction per command, except when already opened 
                // transaction is for write
                if (this.transaction.HasTransaction() && !this.transaction.IsOpenByWrite())
                {
                    this.transaction.Commit();
                    this.transaction.Dispose();
                    this.transaction.SetTransaction(null, openByWrite);
                }
                if (!this.transaction.HasTransaction())
                {
                    if (il.HasValue)
                        this.transaction.SetTransaction(cnx.BeginTransaction(il.Value), openByWrite);
                    else
                        this.transaction.SetTransaction(cnx.BeginTransaction(), openByWrite);
                }
            }
        }

        protected virtual void SetUpConnection(IDbConnection cnx)
        {
            if (shouldOpen && cnx.State != ConnectionState.Open)
                cnx.Open();
            if (databaseName != null)
                cnx.ChangeDatabase(databaseName);
        }

        public virtual IDbConnection GetWritableConnection()
        {
            if (this.writableConnection == null)
            {
                this.writableConnection = this.connectionManager.GetWritableConnection();
                this.connections.Add(this.writableConnection);
            }
            this.SetUpConnection(this.writableConnection);
            this.BeginTransaction(this.writableConnection, true);
            return this.writableConnection;
        }

        public virtual IDbConnection GetConnection()
        {
            // if we were writing we could be in transaction, so we reuse
            // write connection
            if (writableConnection != null)
            {
                this.BeginTransaction(this.writableConnection, true);
                return writableConnection;
            }

            bool isWritableConnection = false;
            var cnx = this.connectionManager.GetReadableConnection(out isWritableConnection);
            this.connections.Add(cnx);

            this.SetUpConnection(cnx);
            this.BeginTransaction(cnx, isWritableConnection);
            return cnx;
        }

        public virtual void ReleaseConnection(IDbConnection cnx)
        {
            // if it's writable connection we keep it open
            // for transaction
            if (cnx == writableConnection)
                return;
            cnx.Close();
            cnx.Dispose();
            this.connections.Remove(cnx);
        }

        public IDbCommand CreateRealCommand()
        {
            var cnx = this.connectionManager.GetReadableConnection();
            return cnx.CreateCommand();
        }

        public IDbTransaction GetCurrentTransaction()
        {
            if (this.transaction != null)
                return this.transaction.GetCurrentTransaction();
            return null;
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            transaction = new ReplicationTransaction(this, il);
            this.il = il;
            return transaction;
        }

        public IDbTransaction BeginTransaction()
        {
            transaction = new ReplicationTransaction(this, IsolationLevel.ReadCommitted);
            return transaction;
        }

        public void ChangeDatabase(string databaseName)
        {
            this.connections.ForEach(c => c.ChangeDatabase(databaseName));
            this.databaseName = databaseName;
        }

        public void Close()
        {
            this.connections.ForEach(c => c.Close());
            shouldOpen = false;
        }

        public string ConnectionString
        {
            get
            {
                var cnx = this.connections.FirstOrDefault();
                if (cnx == null)
                    return string.Empty;
                return cnx.ConnectionString;
            }
            set { }
        }

        public int ConnectionTimeout
        {
            get 
            {
                var cnx = this.connections.FirstOrDefault();
                if (cnx == null)
                    return 0;
                return cnx.ConnectionTimeout;
            }
        }

        public IDbCommand CreateCommand()
        {
            var cmd = new ReplicationCommand(this, cmdTypeAnalyzer);
            cmd.Connection = this;
            return cmd;
        }

        public string Database
        {
            get 
            {
                var cnx = this.connections.FirstOrDefault();
                if (cnx == null)
                    return databaseName;
                return cnx.Database;
            }
        }

        public void Open()
        {
            shouldOpen = true;
        }

        public ConnectionState State
        {
            get 
            {
                if (shouldOpen)
                    return ConnectionState.Open;
                return ConnectionState.Closed;
            }
        }

        public void Dispose()
        {
            this.connections.ForEach(c => c.Dispose());
            this.transaction.Dispose();
            this.connections = new List<IDbConnection>();
        }
    }
}
