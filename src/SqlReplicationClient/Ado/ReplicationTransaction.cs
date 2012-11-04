using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Ado
{
    public class ReplicationTransaction : IDbTransaction
    {
        readonly ReplicationConnection connection;

        protected IsolationLevel il;
        protected IDbTransaction transaction;
        protected bool openByWrite;

        private void ExecuteIfNotNull(Action<IDbTransaction> command)
        {
            if (transaction != null)
                command(transaction);
        }

        public ReplicationTransaction(ReplicationConnection connection,
            IsolationLevel il)
        {
            this.connection = connection;
            this.il = il;
        }

        public IDbConnection Connection
        {
            get
            {
                if (transaction != null)
                    return transaction.Connection;
                return this.connection;
            }
        }

        public IsolationLevel IsolationLevel
        {
            get
            {
                if (transaction != null)
                    return transaction.IsolationLevel;
                return il;
            }
        }

        public void Commit()
        {
            this.ExecuteIfNotNull(c => c.Commit());
        }

        public void Rollback()
        {
            this.ExecuteIfNotNull(c => c.Rollback());
        }

        public virtual void Dispose()
        {
            this.ExecuteIfNotNull(c => c.Dispose());
            transaction = null;
            this.openByWrite = false;
        }

        public void SetTransaction(IDbTransaction dbTransaction, bool openByWrite)
        {
            this.transaction = dbTransaction;
            this.openByWrite = openByWrite;
        }

        public bool HasTransaction()
        {
            return transaction != null;
        }

        public IDbTransaction GetCurrentTransaction()
        {
            return transaction;
        }

        public bool IsOpenByWrite()
        {
            return openByWrite;
        }
    }
}
