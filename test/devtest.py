#   Copyright 2012 Niko Usai <usai.niko@gmail.com>, http://mogui.it
#
#   this file is part of pyorient
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from pyorient import OrientDB, PyOrientException
#from . import getTestConfig

#c = getTestConfig()
#dbname = c['existing_db']

def test_simpleconnection():
  db = OrientDB('127.0.0.1', 2424, "root", "root")
  assert db.session_id > 0

def test_wrongconnect():
  db = OrientDB('127.0.0.1', 2424)
  try:
    db.connect("root", "asder")
  except PyOrientException, e:
    print "Exc: %s" % e
    assert True
  else:
    assert False

# def test_shutdown():
#   db = OrientDB('127.0.0.1', 2424, "root", "root")
#   db.shutdown("root", "root")

def test_dbopen():
  db = OrientDB('127.0.0.1', 2424)
  clusters = db.db_open("GratefulDeadConcerts", "admin", "admin")
  assert clusters != None
  print "Server:      %s" % db.release
  print "Info on db:  %s" % clusters
  assert db.session_id > 0
