import datetime
import os, time

## Email libraries
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

def setup_co(self, *args, **kwds):
  self.cal = False
  self.last_cal_time = None
  self.time_interval_lower = datetime.timedelta(seconds=3300)
  self.time_interval_upper = datetime.timedelta(seconds=3900)
  self.time_late_flag = False

def process_co(self, tm, data):
  coraw_al = data[0]

  if self.last_cal_time is None:
    pass
  elif ((tm - self.last_cal_time) >= self.time_interval_upper
       and self.time_late_flag == False):
    self.pnt("CO cal is late.", tm)
    self.time_late_flag = True
  elif ((tm - self.last_cal_time) < self.time_interval_upper
       and self.time_late_flag == True):
    self.time_late_flag = False

  if coraw_al <= 8000 and self.cal == False:
    self.pnt("CO cal occuring.", tm)

    if self.last_cal_time is None:
      pass
    elif (tm - self.last_cal_time) < self.time_interval_lower:
      self.pnt("CO cal is early.", tm)

    self.last_cal_time = tm
    self.cal = True
  elif coraw_al > 8000 and self.cal == True:
    self.cal = False

def setup_follow(self, *args, **kwds):
  self.time_update = None
  self.time_interval = datetime.timedelta(seconds=3600)
  self.last_coraw_al = 0
  self.last_co_qlive = 0
  self.points = 0
  self.follow_percent = 0

def process_follow(self, tm, data):
  if self.time_update is None:
    self.time_update = tm

  coraw_al = data[0]
  co_qlive = data[1]

  if not(52<=co_qlive<=145):
    return
  elif coraw_al <= 8000:
    return

  coraw_direction = coraw_al - self.last_coraw_al
  co_qlive_direction = co_qlive - self.last_co_qlive

  if coraw_direction > 0:
    coraw_direction = 1
  elif coraw_direction < 0:
    coraw_direction = -1
  else:
    coraw_direction = 0

  if co_qlive_direction > 0:
    co_qlive_direction = 1
  elif co_qlive_direction < 0:
    co_qlive_direction = -1
  else:
    co_qlive_direction = 0

  if coraw_direction == co_qlive_direction:
    self.follow_percent += 1
  else:
    self.follow_percent -= 1

  self.points += 1
  self.last_coraw_al = coraw_al
  self.last_co_qlive = co_qlive

  if (tm - self.time_update) >= self.time_interval:
    self.pnt("coraw_al/co_qlive follow: %d" % self.follow_percent, tm)
    self.time_update = tm



## sendMail in watch module is empty, it must be filled out later in order
## to send emails. This was done because there is no cross platform, cross
## mail server implementation that works to send emails except an SMTP server,
## which many users do not have running on their personal machines.
def sendMail(flight_info=None, files=None):
  """
  Mail function copied from Stack Overflow. Uses gmail account for SMTP server.
  """
  pw = open(".pass", 'r').read().split("\n")
  fro = pw[0]
  to = ("ryano@ucar.edu", "ryan@rdodesigns.com")
  project_name = flight_info['ProjectName']
  flight_number = flight_info['FlightNumber']

  msg = MIMEMultipart()
  msg['From'] = fro
  msg['To'] = ", ".join(to)
  msg['Date'] = formatdate(localtime=True)
  msg['Subject'] = "Data from flight %s_%s" % (project_name, flight_number)

  body = ("Data from flight %s of project %s attached." %
          (flight_number, project_name))
  msg.attach(MIMEText(body))

  ## MIME type attachments accepted by most servers
  for file in files:
      part = MIMEBase('application', "octet-stream")
      part.set_payload(open(file, "rb").read())
      Encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename="%s"'
                     % os.path.basename(file))
      msg.attach(part)

  ## Alternative to storing password in plaintext in git repo.
  server = smtplib.SMTP('smtp.gmail.com', 587)

  ## Send mail, gmail specific
  server.starttls()
  server.login(fro, pw[1])
  server.sendmail(fro, to, msg.as_string())
  server.quit()
