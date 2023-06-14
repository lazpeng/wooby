using System;

namespace wooby.Error;

public class WoobyException : Exception
{
    public WoobyException(string Message) : base(Message) {}
}
