using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient
{
    public class DefaultCommandTypeAnalyser : ICommandTypeAnalyser
    {
        public ReplicationCommandType GetReplicationCommandType(System.Data.IDbCommand cmd, CommandFunction fct)
        {
            switch (fct)
            {
                case CommandFunction.ExecuteNonQuery:
                    return ReplicationCommandType.Write;
                case CommandFunction.ExecuteScalar:
                case CommandFunction.ExecuteReader:
                    return ReplicationCommandType.Read;
            }
            return ReplicationCommandType.Read;
        }
    }
}
