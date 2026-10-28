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
  /** Column headings; strings, or nodes when a host styles them. */
  headers: ReactNode[];
  children: ReactNode;
}) {
  return <StyledRoot tabIndex={0} role="region" aria-label={label}>
    <StyledTable aria-label={label}>
      <StyledTableHead><StyledTableHeadRow>
        {headers.map((header, index) => <StyledTableHeadCell key={index} scope="col">{header}</StyledTableHeadCell>)}
      </StyledTableHeadRow></StyledTableHead>
      <StyledTableBody>{children}</StyledTableBody>
    </StyledTable>
  </StyledRoot>;
}
