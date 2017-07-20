using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Business.Contracts;

namespace Business
{
    public class BreakAt1000MsgDecorator : IMessageHandler
    {
        private readonly IMessageHandler _innerHandler;
        private int _processedMsgCnt = 0;

        public BreakAt1000MsgDecorator(IMessageHandler innerHandler)
        {
            _innerHandler = innerHandler;
        }
        public async Task HandleAsync(Message msg, SqlTransaction tran = null)
        {
            Interlocked.Increment(ref _processedMsgCnt);
            await _innerHandler.HandleAsync(msg, tran);
            if (_processedMsgCnt % 1000 == 0)
                throw new Exception("1000 exception");
        }
    }
}
