from google.appengine.api import mail

def mailIt(email,subject, body):
    mail.send_mail(email,email,subject,body)
