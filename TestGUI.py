from tkinter import *

from CallerFile import *
root = Tk()
root.title("MQTT and Kafka Broker Status")
root.geometry("300x300")
global kafka_status
global mqtt_status

kafka_status=True
mqtt_status=True
def kafka(status):
    Kstatus=status
    #print("Kafka Status is", Kstatus)

def MQTT(status):
    Mstatus=status
    #print("MQTT Status is", Mstatus)


def off_Kafka():
    toggle_photo.configure(file='Toggle Off.dll')
    toggle_button.configure(command=on_Kafka)
    onoff1.configure(text='Kafka Broker Off')
    kafka_status=False
    getKafka(kafka_status)
    kafka(kafka_status)
def on_Kafka():
    toggle_photo.configure(file='Toggle On.dll')
    toggle_button.configure(command=off_Kafka)
    kafka_status=True
    getKafka(kafka_status)
    kafka(kafka_status)
    onoff1.configure(text='Kafka Broker On')

def off_MQTT():
    toggle_photo2.configure(file='Toggle Off.dll')
    toggle_button2.configure(command=on_MQTT)
    mqtt_status=False
    getMQTT(mqtt_status)
    MQTT(mqtt_status)
    onoff2.configure(text='MQTT Broker Off')
def on_MQTT():
    toggle_photo2.configure(file='Toggle On.dll')
    toggle_button2.configure(command=off_MQTT)
    mqtt_status=True
    getMQTT(mqtt_status)
    MQTT(mqtt_status)
    onoff2.configure(text='MQTT Broker On')

toggle_photo = PhotoImage(file='Toggle On.dll')
toggle_button = Button(root,image=toggle_photo,border=0,command=on_Kafka)
toggle_button.place(x=150,y=50)

toggle_photo2 = PhotoImage(file='Toggle On.dll')
toggle_button2 = Button(root,image=toggle_photo2,border=0,command=on_MQTT)
toggle_button2.place(x=150,y=0)



#on / off label
onoff1 = Label(root,text="Kafka Broker On",border=0,font=('bold',18))
onoff1.place(x=250,y=50)
onoff2 = Label(root,text="MQTT Broker On",border=0,font=('bold',18))
onoff2.place(x=250,y=0)
root.mainloop()