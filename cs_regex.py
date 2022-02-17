
What we call a flight number is actually called a flight designator.  This
flight designator consists of three parts: airline designator, flight number
and operational suffix.

These parts have the following format:

Airline code: XX(A)
Flight number: 0(0)(0)(0)
Operational suffix: (A)
Here 0 stands for any number, A for a letter and X for either of those. Everything in parentheses is optional.

So the format of a full flight designator would be: XX(A)0(0)(0)(0)(A)



(?<![\dA-Z])(?!\d{2})([A-Z\d]{2})\s?(\d{2,4})(?!\d)

Details

(?<![\dA-Z]) - no letter or digit right before the current location
(?!\d{2}) - no 2 digits allowed immediately to the right of the current location
[A-Z\d]{2} - 2 digits or letters
\s? - an optional whitespace
\d{2,4} - two, three or four digits
(?!\d) - no digit immediately to the right of the current location is allowed.



The flight can also consist of only 1 number like the BA1 flight. Also the whitespace should be non-capturing:
^([A-Z]{3}|[A-Z\d]{2})(?:\s?)(\d{1,4})$



Data
"^(0?[1-9]|[12][0-9]|3[0-1])(\.|-|/)(0?[1-9]|1[0-2])(\.|-|/)((19|20)?[0-9]{2})$"

Anterior:
"^(0?[1-9]|1[0-9]|2[0-9]|3[0-1])(\.|-|/)(0?[1-9]|1[0-2])(\.|-|/)((19|20)[0-9][0-9])$"
