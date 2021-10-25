using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.IO;

namespace wooby.Database.Persistence.Json
{
    public class JsonContextProvider : IContextProvider
    {
        public void CommitChanges(Context context)
        {
            // Backup data and create json string version of it (set to null so it doesn't get serialized)
            context.CustomSourceDataString = JsonSerializer.Serialize(context.CustomSourceData, typeof(JsonCustomData));
            object data = context.CustomSourceData;
            context.CustomSourceData = null;

            // Write json object to file
            File.WriteAllText(context.DatabaseFilename, JsonSerializer.Serialize(context, typeof(Context)));

            // Restore old data and remove string serialization
            context.CustomSourceData = data;
            context.CustomSourceDataString = null;
        }

        public Context LoadContext(string FullPath)
        {
            var file = File.ReadAllText(FullPath);
            if (string.IsNullOrEmpty(file))
            {
                return null;
            }

            Context ctx = JsonSerializer.Deserialize<Context>(file);
            ctx.CustomSourceData = JsonSerializer.Deserialize<JsonCustomData>(ctx.CustomSourceDataString);
            ctx.CustomSourceDataString = null;

            if ((ctx.CustomSourceData as JsonCustomData).Version != new JsonCustomData().Version)
            {
                // TODO: Upgrade? Check for compatibility?
                throw new Exception("Database created using another backend version");
            }

            return ctx;
        }

        public Context NewContext(string Name, string WorkingDirectory)
        {
            var fullPath = Path.Combine(WorkingDirectory, Name);
            if (new FileInfo(fullPath).Exists)
            {
                throw new Exception("Target file already exists");
            }

            var context = new Context
            {
                CustomSourceData = new JsonCustomData(),
                DatabaseSource = ContextSourceType.Json,
                DatabaseFilename = fullPath
            };

            return context;
        }
    }
}
