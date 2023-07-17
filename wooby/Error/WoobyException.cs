using System;

namespace wooby.Error;

public class WoobyException : Exception
{
    public WoobyException(string message) : base(message) {}
}
