using SqlReplicationClient.Sessions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient.Ado
{
    public class ReplicationCommand : IDbCommand
    {
        protected readonly ReplicationConnection connection;
        protected readonly ICommandTypeAnalyser cmdTypeAnalyzer;

        protected IDbCommand currentCommand = null;

        int? commandTimeout = null;
        CommandType? commandType = null;
        UpdateRowSource? updateRowSource = null;
        bool shouldPrepare = false;

        public ReplicationCommand(ReplicationConnection connection,
            ICommandTypeAnalyser cmdTypeAnalyzer)
        {
            this.connection = connection;
            this.cmdTypeAnalyzer = cmdTypeAnalyzer;
        }

        protected void ExecuteIfNotNull(Action<IDbCommand> command)
        {
            if (currentCommand != null)
                command(currentCommand);
        }

        protected virtual IDbCommand CreateCommand(IDbConnection cnx)
        {
            var cmd = this.InternalCommand();
            cmd.Connection = cnx;
            cmd.Transaction = this.connection.GetCurrentTransaction();
            cmd.CommandText = this.CommandText;
            if (this.commandTimeout.HasValue)
                cmd.CommandTimeout = this.commandTimeout.Value;
            if (this.commandType.HasValue)
                cmd.CommandType = this.commandType.Value;
            if (this.updateRowSource.HasValue)
                cmd.UpdatedRowSource = updateRowSource.Value;
            if (shouldPrepare)
                cmd.Prepare();
            return cmd;
        }

        private IDbCommand InternalCommand()
        {
            if (currentCommand == null)
                currentCommand = this.connection.CreateRealCommand();
            return currentCommand;
        }

        protected virtual IDbConnection GetConnection(ReplicationCommandType cmdType)
        {
            return cmdType == ReplicationCommandType.Read ?
                this.connection.GetConnection() : this.connection.GetWritableConnection();
        }

        public void Cancel()
        {
            this.ExecuteIfNotNull(c => c.Cancel());
        }

        public string CommandText
        {
            get;
            set;
        }

        public int CommandTimeout
        {
            get
            {
                if (!commandTimeout.HasValue)
                    commandTimeout = this.InternalCommand().CommandTimeout;
                return commandTimeout.Value;
            }
            set
            {
                commandTimeout = value;
            }
        }

        public CommandType CommandType
        {
            get
            {
                if (!commandType.HasValue)
                    commandType = this.InternalCommand().CommandType;
                return commandType.Value;
            }
            set
            {
                commandType = value;
            }
        }

        public IDbConnection Connection
        {
            get;
            set;
        }

        public IDbDataParameter CreateParameter()
        {
            return this.InternalCommand().CreateParameter();
        }

        public int ExecuteNonQuery()
        {
            var cmdType = this.cmdTypeAnalyzer.GetReplicationCommandType(this,
                CommandFunction.ExecuteNonQuery);
            var cnx = this.GetConnection(cmdType);
            var cmd = this.CreateCommand(cnx);

            var result = cmd.ExecuteNonQuery();
            this.connection.ReleaseConnection(cnx);
            return result;
        }

        public virtual IDataReader ExecuteReader(CommandBehavior behavior)
        {
            var cmdType = this.cmdTypeAnalyzer.GetReplicationCommandType(this,
                CommandFunction.ExecuteReader);
            var cnx = this.GetConnection(cmdType);
            var cmd = this.CreateCommand(cnx);

            return cmd.ExecuteReader(behavior);
        }

        public virtual IDataReader ExecuteReader()
        {
            var cmdType = this.cmdTypeAnalyzer.GetReplicationCommandType(this,
                CommandFunction.ExecuteReader);
            var cnx = this.GetConnection(cmdType);
            var cmd = this.CreateCommand(cnx);

            return cmd.ExecuteReader();
        }

        public virtual object ExecuteScalar()
        {
            var cmdType = this.cmdTypeAnalyzer.GetReplicationCommandType(this,
                CommandFunction.ExecuteReader);
            var cnx = this.GetConnection(cmdType);
            var cmd = this.CreateCommand(cnx);

            var result = cmd.ExecuteScalar();
            this.connection.ReleaseConnection(cnx);
            return result;
        }

        public IDataParameterCollection Parameters
        {
            get {
                return this.InternalCommand().Parameters; 
            }
        }

        public void Prepare()
        {
            shouldPrepare = true;
        }

        public IDbTransaction Transaction
        {
            get;
            set;
        }

        public UpdateRowSource UpdatedRowSource
        {
            get
            {
                if (!updateRowSource.HasValue)
                    updateRowSource = this.InternalCommand().UpdatedRowSource;
                return updateRowSource.Value;
            }
            set
            {
                updateRowSource = value;
            }
        }

        public void Dispose()
        {
            this.ExecuteIfNotNull(c => c.Dispose());
        }
    }
}
