FROM vformato/pms-ci:ubuntu20.04 as builder

COPY . /PMS/software/source/PMS

RUN cd /PMS/software/source/PMS; mkdir build; cd build; cmake -GNinja -DCMAKE_PREFIX_PATH=/PMS/software/install/mongo-cxx-driver ../ -DCMAKE_INSTALL_PREFIX=/PMS/software/install/PMS; ninja; ninja install


FROM vformato/pms-ci:ubuntu20.04

COPY --from=builder /PMS/software/install/ /PMS/software/install/

ENTRYPOINT /PMS/software/install/PMS/bin/PMSOrchestrator