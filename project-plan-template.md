# Project plan

## Objective

> The objective of our project is to provide analytical datasets from jobs posted in Findwork API and their relation to the population size of where these opportunities are available.

## Consumers

New data engineers who want to know where to look for jobs and which cities have the potential of being less competitive based on their population size.

## Questions

What questions are you trying to answer with your data? How will your data support your users?

Example:

> - Are remote jobs growing over time?
> - What job roles are most common?
> - What are the most common employment types?
> - What is the population of the top 10 cities with more job opportunities? 

## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?


| Source name | Source type | Source documentation |
| - | - | - |
| Population data | csv | TBC |
| findwork API | REST API | [DOCS](https://findwork.dev/developers/#api-key) |

The data available through the API gets updated everytime a new job is posted.

## Solution architecture

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

- What data extraction patterns are you going to be using?
- What data loading patterns are you going to be using?
- What data transformation patterns are you going to be performing?

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram.

Here is a sample solution architecture diagram:

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Breakdown of tasks

How is your project broken down? Who is doing what?

https://trello.com/b/VSAZmRke/kanban-template
