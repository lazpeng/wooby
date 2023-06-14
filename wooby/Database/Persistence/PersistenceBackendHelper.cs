using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence
{
    public static class PersistenceBackendHelper
    {
        public static ITableDataProvider GetTableDataProvider(Context context)
        {
            switch (context.DatabaseSource)
            {
                case ContextSourceType.Json:
                    return new Json.JsonTableDataProvider();
                default:
                    return null;
            }
        }

        public static IContextProvider GetContextProvider(string SourceFile)
        {
            // Try to guess based on the file extension
            var info = new FileInfo(SourceFile);
            if (info.Exists)
            {
                return GetContextProviderForType(info.Extension[1..].ToLower());
            }

            return null;
        }

        public static IContextProvider GetContextProviderForType(string Type)
        {
            return Type switch
            {
                "json" => new Json.JsonContextProvider(),
                _ => null
            };
        }
    }
}
