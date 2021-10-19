using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence
{
    public interface IContextProvider
    {
        Context LoadContext(string FullPath);
        Context NewContext(string Name, string WorkingDirectory);
        void CommitChanges(Context context);
    }
}
