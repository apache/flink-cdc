/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import { NavbarSecondaryMenuFiller } from '@docusaurus/theme-common';
import { useNavbarMobileSidebar } from '@docusaurus/theme-common/internal';
import DocSidebarItems from '@theme/DocSidebarItems';
import SidebarHeader from '@site/src/components/SidebarHeader';
import SidebarFooter from '@site/src/components/SidebarFooter';

function MobileMenu({ sidebar, path }) {
  const sidebarState = useNavbarMobileSidebar();
  return (
    <div className="cdc-mobile-menu">
      <SidebarHeader />
      <ul className="menu__list">
        <DocSidebarItems
          items={sidebar}
          activePath={path}
          level={1}
          onItemClick={(item) => {
            if (item.type === 'link' || item.href) sidebarState.toggle();
          }}
        />
      </ul>
      <SidebarFooter />
    </div>
  );
}

export default function MobileSidebar(props) {
  return <NavbarSecondaryMenuFiller component={MobileMenu} props={props} />;
}
