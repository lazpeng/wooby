using wooby.Parsing;

namespace wooby.Error;

public class WoobyParserException : WoobyException
{
    public WoobyParserException(string message, int at, string currentToken = "") : base(FormatMessage(message, at, currentToken))
    {
    }

    public WoobyParserException(string message, int at, Parser.Token currentToken) : this(message, at,
        currentToken.FullText)
    {
    }

    private static string FormatMessage(string message, int at, string currentToken)
    {
        var near = "";
        if (string.IsNullOrEmpty(currentToken))
        {
            near = $"(near {currentToken})";
        }

        return $"Error at {at} {near} : {message}";
    }
}