// created on 27/12/2002 at 17:05
//
// Author:
// 	Francisco Figueiredo Jr. <fxjrlists@yahoo.com>
//
//	Copyright (C) 2002 The Npgsql Development Team
//	npgsql-general@gborg.postgresql.org
//	http://gborg.postgresql.org/project/npgsql/projdisplay.php
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

using System;
using System.Data;

using Npgsql;
using NpgsqlTypes;

using Xunit;

namespace NpgsqlTests
{

    public class DataReaderTests : BaseClassTests
    {
        protected override IDbConnection TheConnection
        {
            get { return _conn;}
        }
        protected override IDbTransaction TheTransaction {
            get { return _t; }
            set { _t = value; }
        }
        protected virtual string TheConnectionString {
            get { return _connString; }
        }

        protected NpgsqlParameter GetNpDbParam(object dbparam)
        {
            return dbparam as NpgsqlParameter;
        }

/*        [Fact]
        public void TestNew()
        {
            NpgsqlCommand command = new NpgsqlCommand("select * from tablea where field_serial = 4;", TheConnection);

            command.Prepare();
            
            NpgsqlDataReaderNew dr = command.ExecuteReaderNew(CommandBehavior.Default);

            while(dr.Read());
        }*/
        [Fact]
        public void RecordsAffecte()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "insert into tablea(field_int4) values (7); insert into tablea(field_int4) values (8)";

            var dr = command.ExecuteReader() as NpgsqlDataReader;
            try
            {
                Assert.Equal(2, dr.RecordsAffected);
            }
            finally
            {
                dr.Close();
            }
        }

        [Fact]
        public void GetBoolean()
        {
            
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = 4;";

            var dr = command.ExecuteReader();

            dr.Read();
            Boolean result = dr.GetBoolean(4);
            Assert.Equal(true, result);
            dr.Close();
        }


        [Fact]
        public void GetChars()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = 1;";

            var dr = command.ExecuteReader();

            dr.Read();
            Char[] result = new Char[6];


            dr.GetChars(1, 0, result, 0, 6);

            Assert.Equal("Random", new String(result));
            
            dr.Close();
        }
        [Fact]
        public void GetCharsSequential()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = 1;";

            var dr = command.ExecuteReader(CommandBehavior.SequentialAccess);

            dr.Read();
            Char[] result = new Char[6];


            dr.GetChars(1, 0, result, 0, 6);

            Assert.Equal("Random", new String(result));
            
            dr.Close();
        }
           
           
        [Fact]
        public void GetBytes()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select field_bytea from tablef where field_serial = 1;";

            var dr = command.ExecuteReader();

            dr.Read();
            Byte[] result = new Byte[2];


            Int64 a = dr.GetBytes(0, 0, result, 0, 2);

            Assert.Equal('S', (Char)result[0]);
            Assert.Equal('.', (Char)result[1]);
            Assert.Equal(2, a);
            
            dr.Close();
        }        
        

        [Fact]
        public void GetInt32()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = 2;";

            var dr = command.ExecuteReader();

            dr.Read();


            Int32 result = dr.GetInt32(2);

            //ConsoleWriter cw = new ConsoleWriter(Console.Out);

            //cw.WriteLine(result.GetType().Name);
            Assert.Equal(4, result);
            
            dr.Close();
        }


        [Fact]
        public void GetInt16()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tableb where field_serial = 1;";

            var dr = command.ExecuteReader() as NpgsqlDataReader;

            dr.Read();

            Int16 result = dr.GetInt16(1);

            Assert.Equal(2, result);
            
            dr.Close();
        }


        [Fact]
        public void GetDecimal()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tableb where field_serial = 3;";

            var dr = command.ExecuteReader() as NpgsqlDataReader;

            dr.Read();

            Decimal result = dr.GetDecimal(3);


            Assert.Equal(4.2300000M, result);
            
            dr.Close();
        }


        [Fact]
        public void GetDouble()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tabled where field_serial = 2;";

            var dr = command.ExecuteReader() as NpgsqlDataReader;

            dr.Read();

            //Double result = Double.Parse(dr.GetInt32(2).ToString());
            Double result = dr.GetDouble(2);

            Assert.Equal(.123456789012345D, result);
            
            dr.Close();
        }


        [Fact]
        public void GetFloat()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tabled where field_serial = 1;";

            var dr = command.ExecuteReader();

            dr.Read();

            //Single result = Single.Parse(dr.GetInt32(2).ToString());
            Single result = dr.GetFloat(1);

            Assert.Equal(.123456F, result);
            
            dr.Close();
        }

        [Fact]
        public void GetString()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = 1;";

            var dr = command.ExecuteReader();

            dr.Read();

            String result = dr.GetString(1);

            Assert.Equal("Random text", result);
            
            dr.Close();
        }


        [Fact]
        public void GetStringWithParameter()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_text = :value;";

            String test = "Random text";
            NpgsqlParameter param = new NpgsqlParameter();
            param.ParameterName = "value";
            param.DbType = DbType.String;
            //param.NpgsqlDbType = NpgsqlDbType.Text;
            param.Size = test.Length;
            param.Value = test;
            command.Parameters.Add(param);

            var dr = command.ExecuteReader();

            dr.Read();

            String result = dr.GetString(1);

            Assert.Equal(test, result);
            
            dr.Close();
        }

        [Fact]
        public void GetStringWithQuoteWithParameter()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_text = :value;";

            String test = "Text with ' single quote";
            NpgsqlParameter param = new NpgsqlParameter();
            param.ParameterName = "value";
            param.DbType = DbType.String;
            //param.NpgsqlDbType = NpgsqlDbType.Text;
            param.Size = test.Length;
            param.Value = test;
            command.Parameters.Add(param);

            var dr = command.ExecuteReader();

            dr.Read();

            String result = dr.GetString(1);

            Assert.Equal(test, result);
            
            dr.Close();
        }


        [Fact]
        public void GetValueByName()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = 1;";

            var dr = command.ExecuteReader();

            dr.Read();

            String result = (String) dr["field_text"];

            Assert.Equal("Random text", result);
            
            dr.Close();
        }

        [Fact]
        public void GetValueFromEmptyResultset()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_text = :value;";

            String test = "Text single quote";
            NpgsqlParameter param = new NpgsqlParameter();
            param.ParameterName = "value";
            param.DbType = DbType.String;
            //param.NpgsqlDbType = NpgsqlDbType.Text;
            param.Size = test.Length;
            param.Value = test;
            command.Parameters.Add(param);

            var dr = command.ExecuteReader();

            dr.Read();


            Assert.Throws<InvalidOperationException>(() =>
                {
                    // This line should throw the invalid operation exception as the datareader will
                    // have an empty resultset.
                    Console.WriteLine(dr.IsDBNull(1));
                });
        }

        [Fact]
        public void GetInt32ArrayFieldType()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select cast(null as integer[])";
            using (var dr = command.ExecuteReader())
            {
                Assert.Equal(typeof(int[]), dr.GetFieldType(0));
            }
        }
        
        [Fact]
        public void TestMultiDimensionalArray()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select :i";
            command.Parameters.Add(new NpgsqlParameter(":i", (new decimal[,]{{0,1,2},{3,4,5}})));
            using(var dr = command.ExecuteReader())
            {
                dr.Read();
                Assert.Equal(2, (dr[0] as Array).Rank);
                decimal[,] da = dr[0] as decimal[,];
                Assert.Equal(da.GetUpperBound(0), 1);
                Assert.Equal(da.GetUpperBound(1), 2);
                decimal cmp = 0m;
                foreach(decimal el in da)
                    Assert.Equal(el, cmp++);
            }
        }
        
        [Fact]
        public void TestArrayOfBytea()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select get_byte(:i[1], 2)";
            command.Parameters.Add(new NpgsqlParameter(":i", new byte[][]{new byte[]{0,1,2}, new byte[]{3,4,5}}));
            using(var dr = command.ExecuteReader())
            {
                dr.Read();
                Assert.Equal(dr[0], 2);
            }
        }

        [Fact]
        public void TestOverlappedParameterNames()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = :test_name or field_serial = :test_name_long";
            command.Parameters.Add(new NpgsqlParameter("test_name", DbType.Int32, 4, "a"));
            command.Parameters.Add(new NpgsqlParameter("test_name_long", DbType.Int32, 4, "aa"));

            GetNpDbParam(command.Parameters[0]).Value = 2;
            GetNpDbParam(command.Parameters[1]).Value = 3;

            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            dr.Close();
        }
        
        [Fact]
        public void TestOverlappedParameterNamesWithPrepare()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = :test_name or field_serial = :test_name_long";
            command.Parameters.Add(new NpgsqlParameter("test_name", DbType.Int32, 4, "abc_de"));
            command.Parameters.Add(new NpgsqlParameter("test_name_long", DbType.Int32, 4, "abc_defg"));

            GetNpDbParam(command.Parameters[0]).Value = 2;
            GetNpDbParam(command.Parameters[1]).Value = 3;

            command.Prepare();
            
            var dr = command.ExecuteReader();
            Assert.NotNull(dr);
            dr.Close();
        }

        [Fact]
        public void TestNonExistentParameterName()
        {
            Assert.Throws<NpgsqlException>(() =>
            {

                var command = TheConnection.CreateCommand();
                command.CommandText = "select * from tablea where field_serial = :a or field_serial = :aa";
                command.Parameters.Add(new NpgsqlParameter(":b", DbType.Int32, 4, "b"));
                command.Parameters.Add(new NpgsqlParameter(":aa", DbType.Int32, 4, "aa"));

                GetNpDbParam(command.Parameters[0]).Value = 2;
                GetNpDbParam(command.Parameters[1]).Value = 3;

                var dr = command.ExecuteReader();
                Assert.NotNull(dr);
            });
        }

        [Fact]
        public void ReadPastDataReaderEnd()
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                var command = TheConnection.CreateCommand();
                command.CommandText = "select * from tablea;";

                var dr = command.ExecuteReader();

                while (dr.Read()) { }

                Object o = dr[0];
                Assert.NotNull(o);
            });
        }

        [Fact]
        public void IsDBNull()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select field_text from tablea;";

            var dr = command.ExecuteReader();

            dr.Read();
            Assert.Equal(false, dr.IsDBNull(0));
            dr.Read();
            Assert.Equal(true, dr.IsDBNull(0));
            
            dr.Close();
        }

        [Fact]
        public void IsDBNullFromScalar()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select max(field_serial) from tablea;";

            var dr = command.ExecuteReader();

            dr.Read();
            Assert.Equal(false, dr.IsDBNull(0));
            dr.Close();
        }



        [Fact]
        public void TypesNames()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where 1 = 2;";

            var dr = command.ExecuteReader();

            dr.Read();

            Assert.Equal("int4", dr.GetDataTypeName(0));
            Assert.Equal("text", dr.GetDataTypeName(1));
            Assert.Equal("int4", dr.GetDataTypeName(2));
            Assert.Equal("int8", dr.GetDataTypeName(3));
            Assert.Equal("bool", dr.GetDataTypeName(4));

            dr.Close();

            command.CommandText = "select * from tableb where 1 = 2";

            dr = command.ExecuteReader();

            dr.Read();

            Assert.Equal("int4", dr.GetDataTypeName(0));
            Assert.Equal("int2", dr.GetDataTypeName(1));
            Assert.Equal("timestamp", dr.GetDataTypeName(2));
            Assert.Equal("numeric", dr.GetDataTypeName(3));
        }
        
        [Fact]
        public void SingleRowCommandBehaviorSupport()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea";

            var dr = command.ExecuteReader(CommandBehavior.SingleRow);

            
            Int32 i = 0;
            
            while (dr.Read())
                i++;
            
            Assert.Equal(1, i);
        }

        [Fact]
        public void SingleRowForwardOnlyCommandBehaviorSupport()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea";
            Int32 i = 0;

            using (var dr = command.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess))
            {

                while (dr.Read())
                {
                    if (!dr.IsDBNull(0))
                        dr.GetValue(0);
                    if (!dr.IsDBNull(1))
                        dr.GetValue(1);
                    i++;
                }
            }

            Assert.Equal(1, i);
        }
        
        
        [Fact]
        public void SingleRowCommandBehaviorSupportFunctioncall()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "funcb";
            command.CommandType = CommandType.StoredProcedure;

            var dr = command.ExecuteReader(CommandBehavior.SingleRow);

            
            Int32 i = 0;
            
            while (dr.Read())
                i++;
            
            Assert.Equal(1, i);
        }
        
        [Fact]
        public void SingleRowCommandBehaviorSupportFunctioncallPrepare()
        {
            //FIXME: Find a way of supporting single row with prepare.
            // Problem is that prepare plan must already have the limit 1 single row support.
            
            var command = TheConnection.CreateCommand();
            command.CommandText = "funcb()";
            command.CommandType = CommandType.StoredProcedure;
            
            command.Prepare();

            var dr = command.ExecuteReader(CommandBehavior.SingleRow);

            
            Int32 i = 0;
            
            while (dr.Read())
                i++;
            
            Assert.Equal(1, i);
        }
        
        [Fact]
        public void PrimaryKeyFieldsMetadataSupport()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from metadatatest1";
            
            var dr = command.ExecuteReader(CommandBehavior.KeyInfo);

            
            DataTable metadata = dr.GetSchemaTable();
            
            Boolean keyfound = false;
            
            foreach(DataRow r in metadata.Rows)
            {
                if ((Boolean)r["IsKey"] )
                {
                    Assert.Equal("field_pk", r["ColumnName"]);
                    keyfound = true;
                }
                    
            }
            Assert.True(keyfound, "No primary key found!");
            
            dr.Close();
        }
        
        [Fact]
        public void IsIdentityMetadataSupport()
        {
            DoIsIdentityMetadataSupport();
        }
        public virtual void DoIsIdentityMetadataSupport()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from metadatatest1";
            
            var dr = command.ExecuteReader(CommandBehavior.KeyInfo);

            
            DataTable metadata = dr.GetSchemaTable();
            
            Boolean identityfound = false;
            
            foreach(DataRow r in metadata.Rows)
            {
                if ((Boolean)r["IsAutoIncrement"] )
                {
                    Assert.Equal("field_serial", r["ColumnName"]);
                    identityfound = true;
                }
                    
            }
            Assert.True(identityfound, "No identity column found!");
            
            dr.Close();
        }
        
        [Fact]
        public void HasRowsWithoutResultset()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "delete from tablea where field_serial = 2000000";
            
            var dr = command.ExecuteReader() as NpgsqlDataReader;

                        
            Assert.False(dr.HasRows);
        }
   
        [Fact]
        public void ParameterAppearMoreThanOneTime()
        {
            
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea where field_serial = :parameter and field_int4 = :parameter";
            
            command.Parameters.Add(new NpgsqlParameter("parameter", NpgsqlDbType.Integer));
            GetNpDbParam(command.Parameters["parameter"]).Value = 1;

            var dr = command.ExecuteReader() as NpgsqlDataReader;
                        
            Assert.False(dr.HasRows);
            
            dr.Close();
        }
        
        [Fact]
        public void SchemaOnlySingleRowCommandBehaviorSupport()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea";

            var dr = command.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.SingleRow);

            
            Int32 i = 0;
            
            while (dr.Read())
                i++;
            
            Assert.Equal(0, i);
        }
        
        [Fact]
        public void SchemaOnlyCommandBehaviorSupport()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea";

            var dr = command.ExecuteReader(CommandBehavior.SchemaOnly);

            
            Int32 i = 0;
            
            while (dr.Read())
                i++;
            
            Assert.Equal(0, i);
        }
        
        
        [Fact]
        public void SchemaOnlyCommandBehaviorSupportFunctioncall()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "funcb";
            command.CommandType = CommandType.StoredProcedure;

            var dr = command.ExecuteReader(CommandBehavior.SchemaOnly);

            
            Int32 i = 0;
            
            while (dr.Read())
                i++;
            
            Assert.Equal(0, i);
        }
        
        [Fact]
        public void FieldNameDoesntExistOnGetOrdinal()
        {
            Assert.Throws<IndexOutOfRangeException>(() =>
            {
                var command = TheConnection.CreateCommand();
            command.CommandText = "select field_serial from tablea";


                using (var dr = command.ExecuteReader())
                {
                    int idx = dr.GetOrdinal("field_int");
                }
            });
        }
                
        [Fact]
        public void FieldNameDoesntExist()
        {
            Assert.Throws<IndexOutOfRangeException>(() =>
            {
                var command = TheConnection.CreateCommand();
                command.CommandText = "select field_serial from tablea";


                using (var dr = command.ExecuteReader())
                {
                    dr.Read();

                    Object a = dr["field_int"];
                    Assert.NotNull(a);
                }
            });
        }
        
        [Fact]
        public void FieldNameKanaWidthWideRequestForNarrowFieldName()
        {//Should ignore Kana width and hence find the first of these two fields
            var command = TheConnection.CreateCommand();
            command.CommandText = "select 123 as ｦｧｨｩｪｫｬ, 124 as ヲァィゥェォャ";

            using(var dr = command.ExecuteReader())
            {
                dr.Read();
                
                Assert.Equal(dr["ｦｧｨｩｪｫｬ"], 123);
                Assert.Equal(dr["ヲァィゥェォャ"], 123);// Wide version.
            }
        }
        
        [Fact]
        public void FieldNameKanaWidthNarrowRequestForWideFieldName()
        {//Should ignore Kana width and hence find the first of these two fields
            var command = TheConnection.CreateCommand();
            command.CommandText = "select 123 as ヲァィゥェォャ, 124 as ｦｧｨｩｪｫｬ";

            using(var dr = command.ExecuteReader())
            {
                dr.Read();
                
                Assert.Equal(dr["ヲァィゥェォャ"], 123);
                Assert.Equal(dr["ｦｧｨｩｪｫｬ"], 123);// Narrow version.
            }
        }
        
        [Fact]
        public void FieldIndexDoesntExist()
        {
            Assert.Throws<IndexOutOfRangeException>(() =>
            {
                var command = TheConnection.CreateCommand();
                command.CommandText = "select field_serial from tablea";


                using (var dr = command.ExecuteReader())
                {
                    dr.Read();

                    Object a = dr[5];
                    Assert.NotNull(a);
                }
            });
        }

        [Fact]
        public void LoadDataTable()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tableh";
            var dr = command.ExecuteReader();
            DataTable dt = new DataTable();
            dt.Load(dr);
            dr.Close();

            Assert.Equal(5, dt.Columns[1].MaxLength);
            Assert.Equal(5, dt.Columns[2].MaxLength);
        }
        [Fact]
        public void CleansupOkWithDisposeCalls()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tablea";
            using (var dr = command.ExecuteReader())
            {
                dr.Read();
                
                dr.Close();
                using (var upd = TheConnection.CreateCommand())
                {
                    upd.CommandText = "select * from tablea";
                    upd.Prepare();
                }
                
           
            }
            
            
            
            
        }
        
        
        [Fact]
        public void TestOutParameter2()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "testoutparameter2";
            command.CommandType = CommandType.StoredProcedure;
            
            command.Parameters.Add(new NpgsqlParameter("@x", NpgsqlDbType.Integer){ Value = 1 });
            command.Parameters.Add(new NpgsqlParameter("@y", NpgsqlDbType.Integer) { Value = 2});
            command.Parameters.Add(new NpgsqlParameter("@sum", NpgsqlDbType.Integer));
            command.Parameters.Add(new NpgsqlParameter("@product", NpgsqlDbType.Refcursor));
            
            GetNpDbParam(command.Parameters["@sum"]).Direction = ParameterDirection.Output;
            GetNpDbParam(command.Parameters["@product"]).Direction = ParameterDirection.Output;
            
            using (var dr = command.ExecuteReader())
            {
                dr.Read();
                
                Assert.Equal(3, GetNpDbParam(command.Parameters["@sum"]).Value);
                Assert.Equal(2, GetNpDbParam(command.Parameters["@product"]).Value);
                
                
           
            }
            
        }

        [Fact]
        public void GetValueWithNullFields()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select * from tableb";
            using (var dr = command.ExecuteReader())
            {
                dr.Read();

                Boolean result = dr.IsDBNull(2);

                Assert.True(result);



            }




        }

        
        [Fact]
        public void HasRowsGetValue()
        {
            var command = TheConnection.CreateCommand();
            command.CommandText = "select 1";
            using (var dr = command.ExecuteReader() as NpgsqlDataReader)
            {
                Assert.True(dr.HasRows);
                Assert.True(dr.Read());
                Assert.Equal(1, dr.GetValue(0));
            }
        }        
    }

    public class DataReaderTestsV2 : DataReaderTests
    {
        protected override IDbConnection TheConnection {
            get { return _connV2; }
        }
        protected override IDbTransaction TheTransaction {
            get { return _tV2; }
            set { _tV2 = value; }
        }
        protected override string TheConnectionString {
            get { return _connV2String; }
        }
        public override void DoIsIdentityMetadataSupport()
        {
            //Not possible with V2?
        }
    }
}
