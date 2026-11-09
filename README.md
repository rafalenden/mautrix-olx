# mautrix-olx

Bridges OLX's messages to Matrix on its sites in Bulgaria, Kazakhstan, Poland, Portugal, Romania and Ukraine. Each conversation about an advert becomes a DM, named after the person and the advert.

## Signing in

In Servuso, open OLX under Connections, turn it on, choose your country and sign in as you would on the site. The bridge copies the session OLX's site stores for itself, including the refresh token it renews itself with every 15 minutes. There's nothing else to set up, and each account keeps its own site.

This uses the API behind OLX's website, which OLX doesn't document. It can stop working if OLX changes its site, and using it may break OLX's terms, so it's at your own risk on your own account.

## What works

| | From OLX | To OLX |
|---|---|---|
| Text | Yes | Yes |
| Photos and files | Yes (the format hasn't been seen on real data yet) | No: the website uses its own upload service, which the bridge doesn't use |
| Chat history | Yes, as much as OLX returns | |
| Marking a chat as read | Yes | Yes |

The bridge has no replies, reactions, edits or typing indicators, and chats begin on OLX.

## How it behaves

- **New messages take up to 15 seconds.** The bridge polls the conversation list, and fetches a conversation only when it changed. The website has a live WebSocket that the bridge doesn't use yet.
- **Whether the other person has read your message isn't shown.**
- **Sessions renew by themselves.** If OLX ends one, the account shows as needing sign-in again.
- **Signing out only forgets the session here.** It doesn't sign you out of OLX elsewhere.
