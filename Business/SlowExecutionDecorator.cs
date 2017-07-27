using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Business.Contracts;

namespace Business
{
    public class SlowExecutionDecorator : IMessageHandler
    {
        private readonly IMessageHandler _innerHandler;

        public SlowExecutionDecorator(IMessageHandler innerHandler)
        {
            _innerHandler = innerHandler;
        }
        public async Task HandleAsync(Message msg, SqlTransaction tran = null)
        {
            Thread.Sleep(6000);
            await _innerHandler.HandleAsync(msg, tran);
            //await Task.Delay(3000);
        }
    }
}
