
# planned features

## daemon functionality

   X Updating daily data after 23:00 to database
      Updating the charts every night for all active symbols. Daily for months, minutely for yesterday.
      Handle previous versions of the nightly report by moving files from yesterday to an archive directlry with the date.
      Analyze historical data for seasonality, clusters, jump points, ...
      writing parse results to database for offline analysis

   X Updating minutely data every 1 to five minutes to database
      running analysis functions on minutely live-data
      sending notifications for alarm events (desktop or apple push notifications)

## Analysis functions

x searching for jumps of more than x % up or down and logging the event with timsstamp, stock symbol and value to the database
x searchimg for recurring events in daily and minutely data
x analysing average slope of last three ten and fifteen minutes and on increasing incline or decline send buy or sell notifications if five minute jump is larger than x

x predict the optimization changes of a portfolio

- needs fitting to external data
- needs data on portfolio splits
