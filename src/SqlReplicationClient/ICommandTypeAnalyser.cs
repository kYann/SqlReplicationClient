using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicationClient
{
    public enum ReplicationCommandType
    {
        Write,
        Read
    }

    public enum CommandFunction
    {
        ExecuteNonQuery,
        ExecuteScalar,
        ExecuteReader
    }

    public interface ICommandTypeAnalyser
    {
        ReplicationCommandType GetReplicationCommandType(IDbCommand cmd, CommandFunction fct);
    }
}
