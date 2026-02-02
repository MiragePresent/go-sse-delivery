# go-sse-delivery

This is a package designed to simplify Server Side Event messages delivery. It can broadcast data to all connected devices or only to authorized or specific ones.

Example 1:
You building a web page that shows some ads on kiosk screan. You have multiple kiosks. The page on each kiosk just listenes to you SSE endpoint and shows new ad once receives it

Example 2:
Some kiosks are located inside on an openspace office and they display ads and updates regarding company events. Because those kiosks have unique identifiers SSE endpoint sends private events to them as well

Example 3:
An user sends an order request from the kiosk, now the kiosk receives user's order events as well, unless user stops them
