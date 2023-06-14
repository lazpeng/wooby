using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
    public enum ColumnType
    {
        String,
        Number,
        Boolean
    }

    public class ColumnMeta
    {
        public string Name { get; set; }
        public int Id { get; set; }
        public bool Unique { get; set; }
        public ColumnType Type { get; set; }
    }

    public class TableMeta
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public List<ColumnMeta> Columns { get; set; } = new List<ColumnMeta>();
        public bool IsReal { get; set; }
        public bool IsTemporary { get; set; }
    }

    public class Schema
    {
        public string Name { get; set; }
        public bool IsMain { get; set; }
        public List<TableMeta> Tables { get; set; } = new List<TableMeta>();
    }

    public class GlobalVariable
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public bool IsReal { get; set; }
        public ColumnType Type { get; set; }
    }

    public class Context
    {
        public List<Schema> Schemas { get; private set; } = new List<Schema>();
        public List<GlobalVariable> Variables { get; private set; } = new List<GlobalVariable>();

        public Context()
        {
            Schemas.Add(new Schema() { IsMain = true, Name = "main" });
        }

        public TableMeta FindTable(TableReference reference)
        {
            string schema = reference.Schema;
            if (string.IsNullOrEmpty(schema))
            {
                schema = "main";
            }

            var schemaMeta = Schemas.Find(s => s.Name == schema);
            if (schemaMeta != null)
            {
                return schemaMeta.Tables.Find(t => t.Name == reference.Table);
            }

            return null;
        }


        public ColumnMeta FindColumn(ColumnReference reference)
        {
            var tableMeta = FindTable(reference);

            if (tableMeta != null)
            {
                return tableMeta.Columns.Find(c => c.Name == reference.Column);
            }

            return null;
        }

        public GlobalVariable FindVariable(string Name)
        {
            return Variables.Find(v => v.Name == Name);
        }

        public bool IsReferenceValid(ColumnReference reference)
        {
            return FindColumn(reference) != null || (string.IsNullOrEmpty(reference.Table) && FindVariable(reference.Column) != null);
        }

        public bool IsReferenceValid(TableReference reference)
        {
            return FindTable(reference) != null;
        }
    }
}
