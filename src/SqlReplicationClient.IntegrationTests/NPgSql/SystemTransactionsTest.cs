using System;
using System.Data;
using System.Transactions;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace NpgsqlTests
{
    //public class SystemTransactionsTest : BaseClassTests
    //{
    //    protected override IDbConnection TheConnection
    //    {
    //        get { return _conn; }
    //    }
    //    protected override IDbTransaction TheTransaction
    //    {
    //        get { return _t; }
    //        set { _t = value; }
    //    }
    //    protected virtual string TheConnectionString
    //    {
    //        get { return _connString; }
    //    }

    //    [Fact]
    //    public void DistributedTransactionRollback()
    //    {
    //        int field_serial1;
    //        int field_serial2;
    //        using (TransactionScope scope = new TransactionScope())
    //        {
    //            //UseStringParameterWithNoNpgsqlDbType
    //            using (var connection = this.CreateEnlistConnection())
    //            {
    //                connection.Open();
    //                var command = connection.CreateCommand();
    //                command.CommandText = "insert into tablea(field_text) values (:p0)";

    //                command.Parameters.Add(new NpgsqlParameter("p0", "test"));


    //                Assert.Equal((command.Parameters[0] as NpgsqlParameter).NpgsqlDbType, NpgsqlDbType.Text);
    //                Assert.Equal((command.Parameters[0] as NpgsqlParameter).DbType, DbType.String);

    //                Object result = command.ExecuteNonQuery();

    //                Assert.Equal(1, result);


    //                var t = connection.CreateCommand();
    //                t.CommandText = "select max(field_serial) from tablea";
    //                field_serial1 = (int)t.ExecuteScalar();
    //                var command2 = connection.CreateCommand();
    //                command2.CommandText = "select field_text from tablea where field_serial = (select max(field_serial) from tablea)";


    //                result = command2.ExecuteScalar();



    //                Assert.Equal("test", result);
    //            }
    //            //UseIntegerParameterWithNoNpgsqlDbType
    //            using (var connection = this.CreateEnlistConnection())
    //            {
    //                connection.Open();
    //                var command = connection.CreateCommand();
    //                command.CommandText = "insert into tablea(field_int4) values (:p0)";

    //                command.Parameters.Add(new NpgsqlParameter("p0", 5));

    //                Assert.Equal((command.Parameters[0] as NpgsqlParameter).NpgsqlDbType, NpgsqlDbType.Integer);
    //                Assert.Equal((command.Parameters[0] as NpgsqlParameter).DbType, DbType.Int32);


    //                Object result = command.ExecuteNonQuery();

    //                Assert.Equal(1, result);


    //                var t = connection.CreateCommand();
    //                t.CommandText = "select max(field_serial) from tablea";
    //                field_serial2 = (int)t.ExecuteScalar();
    //                var command2 = connection.CreateCommand();
    //                command2.CommandText = "select field_int4 from tablea where field_serial = (select max(field_serial) from tablea)";


    //                result = command2.ExecuteScalar();


    //                Assert.Equal(5, result);

    //                // using new connection here... make sure we can't see previous results even though
    //                // it is the same distributed transaction
    //                var command3 = connection.CreateCommand();
    //                command3.CommandText = "select field_text from tablea where field_serial = :p0";
    //                command3.Parameters.Add(new NpgsqlParameter("p0", field_serial1));

    //                result = command3.ExecuteScalar();


    //                // won't see value of "test" since that's
    //                // another connection
    //                Assert.Equal(null, result);
    //            }
    //            // not commiting here.
    //        }
    //        // This is an attempt to wait for the distributed transaction to rollback
    //        // not guaranteed to work, but should be good enough for testing purposes.
    //        System.Threading.Thread.Sleep(500);
    //        AssertNoTransactions();
    //        // ensure they no longer exist since we rolled back
    //        AssertRowNotExist("field_text", field_serial1);
    //        AssertRowNotExist("field_int4", field_serial2);
    //    }


    //    private void AssertNoTransactions()
    //    {
    //        // ensure no transactions remain
    //        var command = TheConnection.CreateCommand();
    //        command.CommandText = "select count(*) from pg_prepared_xacts where database = :database";
    //        command.Parameters.Add(new NpgsqlParameter("database", TheConnection.Database));
    //        Assert.Equal(0, command.ExecuteScalar());
    //    }

    //    private void AssertRowNotExist(string columnName, int field_serial)
    //    {
    //        var command = TheConnection.CreateCommand();
    //        command.CommandText = "select " + columnName + " from tablea where field_serial = :p0";
    //        command.Parameters.Add(new NpgsqlParameter("p0", field_serial));
    //        object result = command.ExecuteScalar();
    //        Assert.Equal(null, result);
    //    }   

    //    [Fact]
    //    public void TwoDistributedInSequence()
    //    {
    //        DistributedTransactionRollback();
    //        DistributedTransactionRollback();
    //    }

    //    [Fact]
    //    public void ReuseConnection()
    //    {
    //        using (TransactionScope scope = new TransactionScope())
    //        {
    //            using (var connection = this.CreateEnlistConnection(true))
    //            {
    //                connection.Open();
    //                connection.Close();
    //                connection.Open();
    //                connection.Close();
    //            }
    //        }
    //    }
    //}

    //public class SystemTransactionsTestV2 : SystemTransactionsTest
    //{
    //    protected override IDbConnection TheConnection
    //    {
    //        get { return _connV2; }
    //    }
    //    protected override IDbTransaction TheTransaction
    //    {
    //        get { return _tV2; }
    //        set { _tV2 = value; }
    //    }
    //    protected override string TheConnectionString
    //    {
    //        get { return _connV2String; }
    //    }
    //}
}
