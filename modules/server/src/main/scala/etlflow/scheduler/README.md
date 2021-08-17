
## Cron Reference

| Field Name         | Allowed Values       | Allowed Special Characters | 
| -------------------|:--------------------:| --------------------------:|
| Seconds            | 0-59                 | - * /                      |
| Minutes            | 0-59                 | - * /                      |
| Hours              | 0-23                 | - * /                      |
| Day (of month)     | 1-31                 | * ? / L W                  |
| Month              | 1-12 or JAN-DEC      | - * /                      |
| Day (of week)	     | 1-7 or SUN-SAT       | - * ? / L                  |
| Year (optional)    | empty, 1970-2199     | - * /                      |



There are several special characters that are used to specify values:

| Character | Specifies                                           | Notes                      | 
| ----------|:---------------------:| ---------------------------:|
| *         | All values.	                                      | * in the minute field means every minute.                   |
| ?         | No specific value in the day of month and day of week fields.                                          | ? specifies a value in one field, but not the other.                     |
| -         | A range.                                            | 10-12 in the hour field means the script will run at 10, 11 and 12 (noon).                   |
| '         | Additional values.                                  | Typing "MON,WED,FRI" in the day-of-week field means the script will run only on Monday, Wednesday, and Friday.                 |
| /         | Increments.                                         | 0/15 in the seconds field means the seconds 0, 15, 30, and 45. * before the '/' is equivalent to specifying 0 is the value to start with                  |
| #	        | Day of a month.                                     | 6#3 in the day of week field means the third Friday (day 6 is Friday; #3 is the 3rd Friday in the month).                  |
| L         | The last day of a month or week.                    | L means the last day of the month. If used in the day of week field by itself, it means 7 or SAT                      |
| W         | The weekday (Mon-Fri) nearest the specified day.    | Specifying 15W means the CRON job will fire on the nearest weekday to the 15th of the month.                   |


## Cron Examples

These are examples of CRON expressions.

* A run frequency of once at 16:25 on December 18, 2018: 0 25 16 18 DEC ? 2018
* A run frequency of 12:00 PM (noon) every day: 0 0 12 * * ?
* A run frequency of 11:00 PM every weekday night: 0 0 23 ? * MON-FRI
* A run frequency of 10:15 AM every day: 0 15 10 * * ?
* A run frequency of 10:15 AM every Monday, Tuesday, Wednesday, Thursday and Friday: 0 15 10 ? * MON-FRI
* A run frequency of 12:00 PM (noon) every first day of the month: 0 0 12 1 1/1 ? *
* A run frequency of every hour between 8:00 AM and 5:00 PM Monday-Friday: 0 0 8-17 ? * MON-FRI


