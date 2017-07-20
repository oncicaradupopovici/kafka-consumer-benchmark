using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Threading;

namespace Consumer
{
    public class Dao
    {

        private static int _connectionCount = 0;

        private async Task ExecuteNonQueryAsync(string sql)
        {
            var cnx = await GetNewConnectionAsync();

            try
            {
                var cmd = cnx.CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync();
            }
            finally
            {
                ReleaseConnection(cnx);
            }
        }

        public async Task EnsureTableCreated(string consumerGroup)
        {
            var sql =
                $"if not exists (select 1 from sys.tables where name='{consumerGroup}')" + Environment.NewLine +
                $"CREATE TABLE {consumerGroup}(" + Environment.NewLine +
                @"[Id] [int] IDENTITY(1,1) NOT NULL primary key,
	            [Key] [varchar](100) NULL,
	            [Value] [varchar](1000) NOT NULL,
	            [Topic] [varchar](100) NOT NULL,
	            [Partition] [int] NOT NULL,
	            [Offset] [bigint] NOT NULL,
	            [ConsumerId] [varchar](50) NOT NULL,
	            [RecordStamp] [datetime] NOT NULL default(getdate())
            )";

            await ExecuteNonQueryAsync(sql);
        }

        public async Task PersistMessageAsync(Message<string, string> msg, Consumer<string, string> consumer, string consumerGroup)
        {
            var sql =
                $"insert into {consumerGroup}([Key], Value, Topic, Partition, Offset, ConsumerId) " +
                $"select '{msg.Key}', '{msg.Value}', '{msg.Topic}', {msg.Partition}, {msg.Offset.Value}, '{consumer.MemberId}'";

            await ExecuteNonQueryAsync(sql);
        }

        public async Task SimulateADatabaseDelayAsync(int miliseconds)
        {
            var sql =$"WAITFOR DELAY '00:00:00.{miliseconds}'";

            await ExecuteNonQueryAsync(sql);
        }

        public async Task<SqlConnection> GetNewConnectionAsync()
        {
            SqlConnection cnx = null;

            var success = false;
            var retries = 0;
            while (!success)
            {
                try
                {
                    //cnx = new SqlConnection("Server=.;Database=Kafka;Trusted_Connection=True;MultipleActiveResultSets=true;");
                    cnx = new SqlConnection("Server=.;Database=Kafka;Trusted_Connection=True;");
                    await cnx.OpenAsync();
                    success = true;
                }
                catch(Exception ex)
                {
                    if (retries > 10)
                        throw;

                    success = false;
                    retries++;
                    await Task.Delay(100);
                }
            }

            Interlocked.Increment(ref _connectionCount);

            return cnx;
        }

        public void ReleaseConnection(SqlConnection cnx)
        {
            cnx.Close();
            Interlocked.Decrement(ref _connectionCount);
        }

    }
}
