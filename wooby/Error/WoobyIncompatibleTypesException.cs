using System;

namespace wooby.Error;

public class WoobyIncompatibleTypesException : WoobyDatabaseException
{
    public WoobyIncompatibleTypesException(object a, object b) : base(GetMessage(a.GetType(), b.GetType()))
    {
    }

    private static string GetMessage(Type a, Type b)
    {
        var aName = a.Name.Replace("Value", "");
        var bName = b.Name.Replace("Value", "");
        return $"Operation between incompatible types {aName} and {bName}";
    }
}