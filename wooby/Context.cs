using System;
using System.Collections.Generic;
using System.Linq;

using wooby.Database;
using wooby.Database.Persistence;
using wooby.Parsing;

namespace wooby
{
    public enum ColumnType
    {
        String,
        Number,
        Boolean,
        Date,
        Null
    }

    public enum ContextSourceType
    {
        Json
    }

    public class ColumnFlags
    {
        public bool Unique = false;
        public bool Nullable = true;
        public bool PrimaryKey = false;
    }

    public class ColumnMeta
    {
        public string Name { get; set; }
        public int Id { get; set; }
        public ColumnFlags Flags { get; set; } = new ColumnFlags();
        public ColumnType Type { get; set; }
        public string Table { get; set; }
        public long Parent { get; set; }
    }

    public class TableMeta
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public List<ColumnMeta> Columns { get; set; } = new List<ColumnMeta>();
        public bool IsReal { get; set; }
        public bool IsTemporary { get; set; }
        public ITableDataProvider DataProvider { get; set; }

        public TableMeta AddColumn(string ColumnName, ColumnType Type, ColumnFlags flags = null)
        {
            var col = new ColumnMeta { Id = Columns.Count, Name = ColumnName, Parent = this.Id, Type = Type, Flags = flags ?? new ColumnFlags(), Table = Name };
            Columns.Add(col);
            return this;
        }

        public ColumnMeta FindColumn(ColumnReference reference)
        {
            return Columns.Find(c => c.Name.ToUpper() == reference.Column.ToUpper());
        }
    }

    public class Context
    {
        private readonly List<Function> _Functions = new ();
        private readonly List<TableMeta> _Tables = new ();
        
        public IReadOnlyList<Function> Functions
        {
            get { return _Functions; }
        }

        public IReadOnlyList<TableMeta> Tables
        {
            get { return _Tables; }
        }
        
        public string DatabaseFilename { get; set; }
        public ContextSourceType DatabaseSource { get; set; }
        public object CustomSourceData { get; set; }
        public string CustomSourceDataString { get; set; }

        public void AddTable(TableMeta table)
        {
            if (Tables.Any(t => t.Name == table.Name))
            {
                throw new Exception("Duplicate table");
            }

            if (table.DataProvider == null)
            {
                table.DataProvider = PersistenceBackendHelper.GetTableDataProvider(this);
            }
            table.DataProvider.Initialize(this, table);
            
            long id = 0;
            if (Tables.Count > 0)
            {
                id = Tables.Max(t => t.Id) + 1;
            }
            table.Id = id;
            _Tables.Add(table);
        }

        public void ResetNotReal()
        {
            _Tables.RemoveAll(t => !t.IsReal);
        }

        public void AddFunction(Function v)
        {
            _Functions.Add(v);
        }

        public TableMeta FindTable(ColumnReference reference)
        {
            return _Tables.Find(t => t.Name.ToUpper() == reference.Table.ToUpper());
        }

        public TableMeta FindTable(long Id)
        {
            return _Tables.Find(t => t.Id == Id);
        }

        public ColumnMeta FindColumn(ColumnReference reference)
        {
            var tableMeta = FindTable(reference);

            if (tableMeta != null)
            {
                return tableMeta.FindColumn(reference);
            }

            return null;
        }

        public Function FindFunction(string Name)
        {
            return _Functions.Find(v => v.Name.ToUpper() == Name.ToUpper());
        }
    }
}
