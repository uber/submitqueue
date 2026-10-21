"use client";

import {
  StyledRoot,
  StyledTable,
  StyledTableBody,
  StyledTableHead,
  StyledTableHeadCell,
  StyledTableHeadRow,
} from "baseui/table-semantic";
import type { ReactNode } from "react";

export function Table({ label, headers, children }: {
  label: string;
  headers: string[];
  children: ReactNode;
}) {
  return <StyledRoot tabIndex={0} role="region" aria-label={label}>
    <StyledTable aria-label={label}>
      <StyledTableHead><StyledTableHeadRow>
        {headers.map(header => <StyledTableHeadCell key={header} scope="col">{header}</StyledTableHeadCell>)}
      </StyledTableHeadRow></StyledTableHead>
      <StyledTableBody>{children}</StyledTableBody>
    </StyledTable>
  </StyledRoot>;
}
