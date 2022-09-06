import json
import times
import db_sqlite
import httpclient
import asyncdispatch
import strformat
import sequtils
import strutils
import times, os
type
  Reply* = ref object
    subject*: string
    name*: string
    threadId*: int
    posterId*: string
    postId*: int
    board*: string
    post*: string
    replyTo*: int
    comment*: string
    date*: int
proc `$`(reply: Reply): string =
    result = "=============>\n" & "ThreadID: " & $reply.threadId & "\n" & "PostId: " & $reply.postId & "\n" & "OpId: " & $reply.posterId & "\n" & "Name: " & reply.name & "\n" & "Subject: " & reply.subject & "\nComment: " & reply.comment & "\n=============>"

proc initDb(path: string) =
  let db = open(path, "", "", "")
  var smts: seq[SqlQuery]
  let
    posts = sql"""CREATE TABLE IF NOT EXISTS  "Posts" (
    "postId"	INTEGER NOT NULL UNIQUE,
    "board"	TEXT NOT NULL,
    "subject"	TEXT,
    "posterId"	TEXT NOT NULL,
    "posterName"	TEXT NOT NULL,
    "threadId"	INTEGER NOT NULL,
    "filename"	TEXT,
    "date"	INTEGER NOT NULL,
    "comment"	TEXT,
    "replyTo"	INTEGER NOT NULL);
  """

    index = sql"""CREATE INDEX IF NOT EXISTS "postsIndex" ON "Posts" (
    "board"	DESC,
    "threadId"	DESC,
    "postId"	DESC
    );
  """
    namesList = sql"""
    CREATE VIEW IF NOT EXISTS namesListing AS
    SELECT DISTINCT(posterName) FROM Posts WHERE posterName != "Anonymous";
  """
    threadsList = sql"""
    CREATE VIEW IF NOT EXISTS threadListing AS
    SELECT Subject, posterName, comment, threadId FROM Posts WHERE replyTo = 0;
    """
    namedPostsListing = sql"""
    CREATE VIEW IF NOT EXISTS namesPostsListing AS
    SELECT * FROM Posts WHERE posterName != "Anonymous";
    """

  db.exec(posts)
  db.exec(index)
  db.exec(namesList)
  db.exec(threadsList)
  db.exec(namedPostsListing)
  db.close()
proc insertPost*(db: DbConn, replies: seq[Reply]) =
  for chunk in replies.distribute(num=20, spread=false):
    for reply in chunk:
      when defined(debug):
        echo $chunk[0]
      try:
        db.exec(sql"INSERT INTO Posts(threadId, postId, posterId, replyTo, board, subject, posterName, comment, date) VALUES(?,?,?,?,?,?,?,?,?);",
                reply.threadId, reply.postId, reply.posterId, reply.replyTo, reply.board, reply.subject, reply.name, reply.comment, reply.date)
        db.exec(sql"COMMIT;")
      except DbError:
        discard

proc getThreads(client: AsyncHttpClient, board: string, page=1): Future[seq[string]] {.async.} =
  let url = fmt"https://a.4cdn.org/{board}/threads.json"
  when defined(debug):
    echo(url)
  var resp = await client.getContent(url)
  when defined(debug):
    echo resp
  let j = resp.parseJson
  var threadList: seq[string]
  var pageNo: int
  for page in j.getElems:
    for thread in page["threads"].getElems:
      if thread["replies"].getInt > 0:
        let no = thread["no"].getInt
        threadList.add($no)
    pageNo += 1
    when defined(debug):
      echo $pageNo
  result = threadList

proc scrapeThread(client: AsyncHttpClient, thread: string, board: string): Future[seq[Reply]] {.async.} =
  let url = fmt"https://a.4cdn.org/{board}/thread/{thread}.json"
  var replies: seq[Reply]
  when defined(debug):
    echo(url)
  var resp = await client.get(url)
  when defined(debug):
    echo (await resp.body)
  let j = (await resp.body).parseJson
  when defined(debug):
    echo $j
  for post in j["posts"].getElems:
    var
      comment: string
      subject: string
      name: string
      posterId: string

    let date = post["time"].getInt
    let postNo = post["no"].getInt
    try:
      posterId = post["id"].getStr
    except KeyError:
      return replies
    try:
      name = post["name"].getStr
    except KeyError:
      name = ""
      when defined(debug):
        echo "Error on thread: " & $thread
    try:
      subject = post["sub"].getStr
    except KeyError:
      subject = ""
    try:
      comment = post["com"].getStr
    except KeyError:
      comment = ""
    let replyTo = post["resto"].getInt
    let reply = Reply(subject: subject, name: name, threadId: thread.parseInt,
                      comment: comment, postId: postNo, posterId: posterId, board: board, date: date, replyTo: replyTo)
    when defined(debug):
      echo "Test!"
      echo $reply
    replies.add(reply)
  result = replies

proc scrapeBoard*(client: AsyncHttpClient, board: string, db: DbConn): seq[Reply] =
  var replies, resp: seq[Reply]
  let threads = waitFor client.getThreads(board)
  var threadI: int
  sleep(1000)
  for thread in threads:
    try:
      let resp = waitFor client.scrapeThread(thread, board)
      db.insertPost(resp)
      sleep(1000)
      echo $threadI & "/" & $threads.len
      threadI += 1
    except JsonParsingError:
      echo "Rate limited!, sleeping..."
      sleep(150000)
      echo "done sleeping, back to shill hunting"
  result = replies

proc main(board="pol", dbPath="posts.db") =
  initDb(dbPath)
  var db = open(dbPath, "", "", "")
  var client = newAsyncHttpClient()
  discard client.scrapeBoard("pol", db)
  db.close()
when isMainModule:
  import cligen; dispatch main
