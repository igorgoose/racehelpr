# Racehelpr Scraper

This application scrapes messages from [Kart Chrono](https://kartchrono.com) API. 

## Purpose

The purpose of the project is to gather enough data to build an app that would give
a better analytical insight into the current situation during a race.

## Features 

Scraping is controlled using `scraper tasks`. `Scraper tasks` can be scheduled at a certaing
point in time, cancelled if not in progress or completed, and viewed.

The messages scraped then can be reproduced at `/mockserver` endpoint. Numerous message flow
control features are implemented for better debugging experience.

To quickly navigate between certain points in the message flow labels are implemented.
The first message of each task is marked with the label specified while scheduling the task.
Labels can be arbitrarily added later if needed during reproducing the message flow.

Message flow control features include:
- Emitting messages in real time mode
- Controlling the rate of messages emission
- Fast forwarding/emitting a certain number of messages
- Fast forwarding/emitting messages until the specified label
- Pausing and resuming emission
- Assigning a label to the last emitted message

## Technical Overview

Apache Kafka is used for storing and reproducing message flow.

Labels are stored in a plain text file.

Spring Boot + WebFlux is used for server implementation.
