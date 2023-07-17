using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence;

public interface IContextProvider
{
    Context LoadContext(string fullPath);
    Context NewContext(string name, string workingDirectory);
    void CommitChanges(Context context);
}