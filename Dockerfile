FROM python:3.8-buster


RUN apt-get update -y && apt-get install -y \
    libssl-dev wget \
    fonts-liberation libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libatspi2.0-0 libcups2 \
    libgbm1 libgtk-3-0 libnspr4 libnss3 \
    libxkbcommon0 libvulkan1 \
    sudo xdg-utils libu2f-udev && \
    apt-get autoremove --purge -y && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_current.x | bash - && \
 apt-get install -y nodejs

#dumb init is a supervisor which helps ensure reaping of dead chromes etc
RUN wget https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64.deb
# RUN wget https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_arm64.deb
RUN dpkg -i dumb-init_*.deb

# Set env variables.
ENV SRC=.
ENV SRVHOME=/srv
ENV SRVPROJ=/srv/printbot

RUN pip3 install --upgrade pip

COPY ${SRC}/requirements.txt ${SRVPROJ}/requirements.txt
COPY ${SRC}/renderer/package.json ${SRVPROJ}/renderer/package.json
COPY ${SRC}/renderer/package-lock.json ${SRVPROJ}/renderer/package-lock.json
COPY ${SRC}/renderer/tsconfig.json ${SRVPROJ}/renderer/tsconfig.json
COPY ${SRC}/renderer/lerna.json  ${SRVPROJ}/renderer/lerna.json

WORKDIR ${SRVPROJ}/renderer
RUN npm install

# append source files
COPY ${SRC}/renderer/packages ${SRVPROJ}/renderer/packages
ADD ${SRC}/renderer/src ${SRVPROJ}/renderer/src/
RUN npm run bootstrap
RUN npm run compile && npm run build

WORKDIR ${SRVPROJ}
RUN pip3 install -r requirements.txt
RUN playwright install
# RUN playwright install chrome


ADD ${SRC}/print-bot.py ${SRVPROJ}/print-bot.py
ADD ${SRC}/print-bot-threaded.py ${SRVPROJ}/print-bot-threaded.py

ENV TZ=Australia/Sydney
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ADD $SRC $SRVPROJ
WORKDIR $SRVPROJ
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["python", "print-bot.py"]