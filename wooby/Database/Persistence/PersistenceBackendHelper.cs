using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence;

public static class PersistenceBackendHelper
{
    public static ITableDataProvider GetTableDataProvider(Context context)
    {
        return context.DatabaseSource switch
        {
            ContextSourceType.Json => new Json.JsonTableDataProvider(),
            _ => (ITableDataProvider)null
        };
    }

    public static IContextProvider GetContextProvider(string sourceFile)
    {
        // Try to guess based on the file extension
        var info = new FileInfo(sourceFile);
        return info.Exists ? GetContextProviderForType(info.Extension[1..].ToLower()) : null;
    }

    public static IContextProvider GetContextProviderForType(string type)
    {
        return type switch
        {
            "json" => new Json.JsonContextProvider(),
            _ => null
        };
    }
}