using wooby.Parsing;

namespace wooby.Error;

public class WoobyParserException : WoobyException
{
    public WoobyParserException(string Message, int At, string CurrentToken = "") : base(FormatMessage(Message, At, CurrentToken))
    {
    }

    public WoobyParserException(string Message, int At, Parser.Token CurrentToken) : this(Message, At,
        CurrentToken.FullText)
    {
    }

    private static string FormatMessage(string Message, int At, string CurrentToken)
    {
        var near = "";
        if (string.IsNullOrEmpty(CurrentToken))
        {
            near = $"(near {CurrentToken})";
        }

        return $"Error at {At} {near} : {Message}";
    }
}