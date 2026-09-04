# Privacy Policy

**Last updated: 4 September 2026**

Expense Tracker is a personal portfolio project, not a commercial service. It
is run by an individual developer and offered free of charge. This document
describes exactly what it stores and who can see it.

## What is collected

**Your email address.** When you sign in with Google, Google tells the app your
email address and nothing else. The app never sees your Google password.

**The expenses you enter.** Date, amount, category, subcategory, and any note
you add.

**Your conversation history.** The messages you exchange with the assistant are
stored so a conversation continues across page refreshes and restarts.

Nothing else is collected. There is no analytics, no advertising, no tracking
across other websites, and no cookies beyond the one that keeps you signed in.

## How your email is used

Solely as the identifier that keeps your data separate from other users'.
Every expense row and every conversation is labelled with your email, and every
database query filters on it, so other users cannot read your records.

You will not receive email from this app.

## Who your data is shared with

Nothing is sold, and nothing is shared for advertising. The app depends on four
services to function, each of which processes data on its behalf:

| Service | What it receives | Why |
|---|---|---|
| **Google Sign-In** | your sign-in | authenticates you; the app never sees your password |
| **Google Gemini API** | the text of your messages and the tool results | generates the assistant's replies |
| **Neon** (Postgres, Singapore) | your email, expenses and conversation history | stores them |
| **Prefect Horizon** | your requests to the expense server | hosts the server |
| **Streamlit Community Cloud** | your web session | hosts the web interface |

Your messages are sent to Google's Gemini API to produce replies. If you would
rather not send something to a third-party model, do not type it here.

## Where your data is stored

In a Neon Postgres database hosted in the AWS Asia Pacific (Singapore) region.

## Deleting your data

Individual expenses can be deleted in the app — ask the assistant to delete
them, or say which one you mean.

To delete your account and all associated data, email
**mhopareprathmesh5@gmail.com** from the address you signed in with, and it
will be removed from the database.

## Retention

Data is kept until you delete it or ask for your account to be removed. As a
personal project, the service may be shut down at any time, at which point the
database and everything in it is deleted.

## Security

Connections use TLS. Your data is separated from other users' by an
authenticated identity check on every read and write, not merely by the
interface hiding it. That said, this is a personal project run by one person
and not independently audited — please do not store anything sensitive in it.

## Children

This app is not intended for use by children under 13.

## Changes

Any change to this policy will be committed to
[this repository](https://github.com/mhopareprathmesh5-creator/expense-tracker-mcp),
whose history is public, so the full record of revisions is visible.

## Contact

**mhopareprathmesh5@gmail.com**
