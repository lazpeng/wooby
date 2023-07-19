using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.IO;

namespace wooby.Database.Persistence.Json;

public class JsonContextProvider : IContextProvider
{
    public void CommitChanges(Context context)
    {
        // Backup data and create json string version of it (set to null so it doesn't get serialized)
        context.CustomSourceDataString = JsonSerializer.Serialize(context.CustomSourceData, typeof(JsonCustomData));
        var data = context.CustomSourceData;
        context.CustomSourceData = null;

        // Write json object to file
        File.WriteAllText(context.DatabaseFilename, JsonSerializer.Serialize(context, typeof(Context)));

        // Restore old data and remove string serialization
        context.CustomSourceData = data;
        context.CustomSourceDataString = "";
    }

    public Context? LoadContext(string fullPath)
    {
        var file = File.ReadAllText(fullPath);
        if (string.IsNullOrEmpty(file))
        {
            return null;
        }

        var ctx = JsonSerializer.Deserialize<Context>(file);
        if (ctx == null)
            return null;
        ctx.CustomSourceData = JsonSerializer.Deserialize<JsonCustomData>(ctx.CustomSourceDataString);
        ctx.CustomSourceDataString = "";

        if (ctx.CustomSourceData != null && ((JsonCustomData)ctx.CustomSourceData).Version != new JsonCustomData().Version)
        {
            // TODO: Upgrade? Check for compatibility?
            throw new Exception("Database created using another backend version");
        }

        return ctx;
    }

    public Context NewContext(string name, string workingDirectory)
    {
        var fullPath = Path.Combine(workingDirectory, name);
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