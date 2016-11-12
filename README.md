# s3readme

# Goals

## Init

Scan an existing account and reproduce the folder structure locally.

## Write

Initially we will simply overwrite what is in s3 with what we have locally (sync is future feature)

## Sync

*rsync* local README files to s3.  Simple *newest* file wins

add `text/plain` so README's can easily be read in browser w/o download
