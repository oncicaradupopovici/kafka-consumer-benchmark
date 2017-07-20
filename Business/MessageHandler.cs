using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Business.Contracts;


namespace Business
{
    public class MessageHandler : IMessageHandler
    {
        private IDao _dao;

        public MessageHandler(IDao dao)
        {
            this._dao = dao;
        }

        public async Task HandleAsync(Message msg, SqlTransaction tran = null)
        {
            Console.WriteLine($"Message received {msg.Value}");

            await _dao.SimulateADatabaseDelayAsync(10, tran); //simulate some async wait - like a database read

            System.Threading.Thread.Sleep(10); //simulate some synchronuos work

            //Console.WriteLine($"Message processed on thread {System.Threading.Thread.CurrentThread.ManagedThreadId}");
            await _dao.PersistMessageAsync(msg, tran);
            
        }
    }
}
