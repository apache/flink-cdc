/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.utils;

/** Used to compare version numbers at runtime. */
public class VersionComparable implements Comparable<VersionComparable> {

    private int majorVersion;
    private int minorVersion;
    private int patchVersion;
    private String versionString;

    private VersionComparable(String versionString) {
        this.versionString = versionString;
        try {
            int pos = versionString.indexOf('-');
            String numberPart = versionString;
            if (pos > 0) {
                numberPart = versionString.substring(0, pos);
            }

            String[] versions = numberPart.split("\\.");

            this.majorVersion = Integer.parseInt(versions[0]);
            this.minorVersion = Integer.parseInt(versions[1]);
            if (versions.length == 3) {
                this.patchVersion = Integer.parseInt(versions[2]);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Can not recognize version %s.", versionString));
        }
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int getPatchVersion() {
        return patchVersion;
    }

    public static VersionComparable fromVersionString(String versionString) {
        return new VersionComparable(versionString);
    }

    @Override
    public int compareTo(VersionComparable version) {
        if (equalTo(version)) {
            return 0;
        } else if (newerThan(version)) {
            return 1;
        } else {
            return -1;
        }
    }

    public boolean equalTo(VersionComparable version) {
        return majorVersion == version.majorVersion
                && minorVersion == version.minorVersion
                && patchVersion == version.patchVersion;
    }

    public boolean newerThan(VersionComparable version) {
        if (majorVersion <= version.majorVersion) {
            if (majorVersion < version.majorVersion) {
                return false;
            } else {
                if (minorVersion <= version.minorVersion) {
                    if (minorVersion < version.patchVersion) {
                        return false;
                    } else {
                        return patchVersion > version.patchVersion;
                    }
                }
            }
        }
        return true;
    }

    public boolean newerThanOrEqualTo(VersionComparable version) {
        return newerThan(version) || equalTo(version);
    }

    @Override
    public String toString() {
        return versionString;
    }
}
